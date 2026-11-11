#!/usr/bin/env python3
"""Repeatable local HTTP demonstration. Does not reset data or sidecar settings."""
import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:8080')
    parser.add_argument('--pause', action='store_true', help='Wait between recording chapters')
    args = parser.parse_args()
    # A new user namespace makes repeated recordings safe: no existing data is reset.
    uid = 'demo-' + uuid.uuid4().hex[:10]
    base = args.url.rstrip('/')
    user = '/v1/users/' + uid
    shared = 'Ana is leading the Atlas project.'
    budget = 'The Atlas project budget is 120000 dollars.'
    oldDeadline = 'The Atlas project deadline is March 3.'
    newDeadline = 'The Atlas project deadline is April 4.'
    member = 'Ben joined the Atlas project.'

    # Keep the demonstration on the public HTTP boundary, just like a real client.
    # No direct database writes, sidecar admin settings, or hidden model shortcuts.
    def request(method, path, body=None, allow404=False):
        payload = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base + path, data=payload, method=method,
                                     headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and allow404:
                return None
            raise

    def event(source, item, version, content):
        return request('POST', '/v1/events', dict(user_id=uid, source_id=source,
            item_id=item, version=version, kind='upsert', content=content,
            occurred_at='2026-11-11T00:00:00Z'))

    def memories():
        return request('GET', user + '/memories?limit=500')['memories']

    def atomic(rows, text):
        return next(m for m in rows if m['kind'] == 'atomic' and m['text'] == text)

    def summaries(rows):
        return [m for m in rows if m['kind'] == 'summary']

    # Writes promise durable acceptance, not immediate extraction. Poll until the
    # complete expected logical state is visible, with a bounded failure deadline.
    def waitFor(texts, wantSummary=True):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            rows = memories()
            actual = {m['text'] for m in rows if m['kind'] == 'atomic'}
            sums = summaries(rows)
            if actual == set(texts) and len(sums) == int(wantSummary):
                if not sums or all(text in sums[0]['text'] for text in texts):
                    return rows
            time.sleep(0.1)
        raise AssertionError('Did not converge within 20 seconds: ' + json.dumps(rows))

    def show(title, payload):
        print('\n=== ' + title + ' ===', flush=True)
        print(json.dumps(payload, indent=2), flush=True)
        if args.pause:
            input('Press Enter to continue... ')

    # Forgetting has a stronger promise than ingestion: check all three read paths
    # immediately after the delete response, without allowing convergence time.
    def assertClean(forbiddenText, forbiddenItem, oldId=None):
        # These requests start only after the delete response, without a wait.
        rows = memories()
        recalled = request('GET', user + '/recall?q=' + urllib.parse.quote(forbiddenText) + '&k=100')['memories']
        one = request('GET', user + '/memories/' + oldId, allow404=True) if oldId else None
        for memory in rows + recalled + ([one] if one else []):
            assert forbiddenText not in memory['text'], memory
            assert forbiddenItem not in memory['lineage']['items'], memory
        if oldId:
            assert one is None, one
        return rows

    request('GET', '/v1/health')
    print('Fresh demonstration user:', uid, flush=True)
    # Two independent sources state exactly the same fact. The unique atomic
    # memory should preserve both source-item edges instead of duplicating text.
    event('gmail', 'shared-email', 1, shared)
    event('gcal', 'shared-calendar', 1, shared)
    event('gmail', 'details', 1, oldDeadline + ' ' + budget)
    event('gcal', 'member', 1, member)
    rows = waitFor([shared, oldDeadline, budget, member])
    originalShared = atomic(rows, shared)
    originalBudget = atomic(rows, budget)
    assert set((i['source_id'], i['item_id']) for i in originalShared['lineage']['items']) == {
        ('gmail', 'shared-email'), ('gcal', 'shared-calendar')}
    show('One shared fact, two source items, one summary', {'shared': originalShared, 'summary': summaries(rows)[0]})

    # Replace only the deadline. Keeping the budget in this edited item lets us
    # prove that unchanged facts retain their IDs even while summaries change.
    event('gmail', 'details', 2, newDeadline + ' ' + budget)
    rows = waitFor([shared, newDeadline, budget, member])
    assert atomic(rows, budget)['id'] == originalBudget['id']
    assert atomic(rows, shared)['id'] == originalShared['id']
    assert oldDeadline not in summaries(rows)[0]['text']
    show('Edit updates the summary; unchanged fact IDs stay stable', {
        'budget_id_before': originalBudget['id'], 'budget_id_after': atomic(rows, budget)['id'],
        'summary': summaries(rows)[0]})

    # Removing one backer must not erase a fact still supported by another one.
    request('DELETE', user + '/items/gmail/shared-email')
    rows = memories()
    remaining = atomic(rows, shared)
    assert remaining['id'] == originalShared['id']
    assert remaining['lineage']['items'] == [{'source_id': 'gcal', 'item_id': 'shared-calendar'}]
    for memory in rows:
        assert {'source_id': 'gmail', 'item_id': 'shared-email'} not in memory['lineage']['items']
    show('Forget the first source: the shared memory remains', remaining)

    # Removing the final backer must immediately hide both the atomic and any
    # summary containing its text; only the remaining three facts are re-summarized.
    request('DELETE', user + '/items/gcal/shared-calendar')
    clean = assertClean(shared, {'source_id': 'gcal', 'item_id': 'shared-calendar'}, originalShared['id'])
    show('Forget the last source: list, recall and direct lookup are immediately clean', {
        'deleted_memory_id': originalShared['id'], 'live_memory_count': len(clean),
        'checks': ['list has no forgotten text', 'recall has no forgotten text', 'direct lookup returns 404']})
    rows = waitFor([newDeadline, budget, member])
    show('The remaining three facts get a fresh summary', summaries(rows)[0])

    # Disconnect is reversible; a user-delete tombstone is not. Reconnect restores
    # the ordinary calendar item but must never restore its forgotten neighbor.
    request('POST', user + '/sources/gcal/disconnect', {})
    disconnected = memories()
    assert member not in {m['text'] for m in disconnected}
    assert not summaries(disconnected)
    request('POST', user + '/sources/gcal/connect', {})
    # Replaying a forgotten item at both the old and a higher version cannot restore it.
    event('gcal', 'shared-calendar', 1, shared)
    event('gcal', 'shared-calendar', 99, shared)
    event('gcal', 'member', 1, member)
    rows = waitFor([newDeadline, budget, member])
    assertClean(shared, {'source_id': 'gcal', 'item_id': 'shared-calendar'}, originalShared['id'])
    show('Reconnect restores the live item, never the permanently forgotten one', {
        'restored_item': atomic(rows, member), 'forgotten_old_and_newer_versions_rejected': True,
        'summary': summaries(rows)[0]})
    print('\nPASS: all HTTP demo assertions passed. Next: backfill and crash harness chapters.', flush=True)


if __name__ == '__main__':
    main()
