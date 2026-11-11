"""Killing and restarting the candidate's service for the crash scenario."""

from __future__ import annotations

import shutil
import subprocess

PERSISTENT_LABEL = "takehome.persistent"


class ProcessControl:
    """Uses docker compose by default. --kill-cmd and --start-cmd replace it, for running without Docker."""

    def __init__(self, compose_dir: str, kill_cmd: str | None = None, start_cmd: str | None = None):
        self.compose_dir = compose_dir
        self.kill_cmd = kill_cmd
        self.start_cmd = start_cmd

    def unavailable_reason(self) -> str | None:
        if bool(self.kill_cmd) != bool(self.start_cmd):
            return "pass both --kill-cmd and --start-cmd, or neither"
        if self.kill_cmd:
            return None
        if shutil.which("docker") is None:
            return "docker was not found; install it or pass --kill-cmd and --start-cmd"
        return None

    def kill(self) -> None:
        if self.kill_cmd:
            subprocess.run(self.kill_cmd, shell=True, check=False, capture_output=True)
            return
        ids = self._docker(["compose", "ps", "-q"]).split()
        for container_id in ids:
            label = self._docker(
                ["inspect", "--format", f'{{{{ index .Config.Labels "{PERSISTENT_LABEL}" }}}}', container_id]
            ).strip()
            if label != "true":
                self._docker(["kill", "--signal", "SIGKILL", container_id])

    def start(self) -> None:
        if self.start_cmd:
            subprocess.Popen(
                self.start_cmd,
                shell=True,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        self._docker(["compose", "up", "-d"])

    def _docker(self, args: list[str]) -> str:
        result = subprocess.run(
            ["docker", *args], cwd=self.compose_dir, check=False, capture_output=True, text=True
        )
        return result.stdout
