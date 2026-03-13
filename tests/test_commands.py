import data_tracker.commands as commands
from click.testing import CliRunner


def test_delete_tracker_no_tracker(monkeypatch):
    """delete-tracker exits with error when tracker is not initialized."""
    runner = CliRunner()
    monkeypatch.setattr(commands.fu, "find_data_tracker_root", lambda: None)

    result = runner.invoke(commands.delete_tracker)

    assert result.exit_code == 1
    assert "not initialized" in result.output


def test_delete_tracker_dry_run_skips_confirmation(monkeypatch):
    """--dry-run should not prompt for destructive confirmation."""
    runner = CliRunner()

    monkeypatch.setattr(commands.fu, "find_data_tracker_root", lambda: "C:/tmp/.data_tracker")

    def _confirm_should_not_be_called(*args, **kwargs):
        raise AssertionError("click.confirm should not be called for --dry-run")

    monkeypatch.setattr(commands.click, "confirm", _confirm_should_not_be_called)
    monkeypatch.setattr(
        commands.fu,
        "delete_data_tracker",
        lambda tracker_path, dry_run: (True, f"dry-run on {tracker_path}={dry_run}"),
    )

    result = runner.invoke(commands.delete_tracker, ["--dry-run"])

    assert result.exit_code == 0
    assert "dry-run on C:/tmp/.data_tracker=True" in result.output


def test_delete_tracker_real_run_confirms(monkeypatch):
    """Without --dry-run, command should confirm before deleting."""
    runner = CliRunner()
    calls = {"confirm": 0}

    monkeypatch.setattr(commands.fu, "find_data_tracker_root", lambda: "C:/tmp/.data_tracker")

    def _confirm(*args, **kwargs):
        calls["confirm"] += 1
        return True

    monkeypatch.setattr(commands.click, "confirm", _confirm)
    monkeypatch.setattr(
        commands.fu,
        "delete_data_tracker",
        lambda tracker_path, dry_run: (True, "deleted"),
    )

    result = runner.invoke(commands.delete_tracker)

    assert result.exit_code == 0
    assert calls["confirm"] == 1
    assert "deleted" in result.output

