"""Health decoder resources must close on completion, error, and explicit interrupt."""
from types import SimpleNamespace

import pytest

from runner import main
from runner.env import Env


@pytest.mark.parametrize('failure', [None, RuntimeError, KeyboardInterrupt])
def test_run_releases_health_before_spark_and_db(monkeypatch, tmp_path, failure):
    calls = []
    run = main.Run.__new__(main.Run)
    run.env = Env()
    run.args = SimpleNamespace(force=False)
    run.sc = SimpleNamespace(streams=[], runner=SimpleNamespace(n_slots=0), expire=None)
    run.run_id = 'cleanup-control'
    run.journal_dir = tmp_path / run.run_id
    run.intents = []
    run._health = None
    run._golden = lambda: ({}, {})
    db = SimpleNamespace(migrate=lambda: None, query=lambda *_a: [], delete_run=lambda *_a: None,
                         update_run=lambda *_a, **_kw: None, close=lambda: calls.append('db'))
    spark = SimpleNamespace(stop=lambda: calls.append('spark'))
    monkeypatch.setattr(main, 'DB', lambda *_a: db)
    monkeypatch.setattr(main, 'build_session', lambda *_a, **_kw: spark)

    def execute(*_args):
        run._health = SimpleNamespace(close=lambda: calls.append('health'))
        if failure:
            raise failure('controlled interruption')
        return 0

    run._execute = execute
    if failure:
        with pytest.raises(failure):
            run.execute()
    else:
        assert run.execute() == 0
    assert calls == ['health', 'spark', 'db']
