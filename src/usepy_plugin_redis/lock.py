from contextlib import contextmanager
from pathlib import Path


class Lock:
    def __init__(self, client):
        self.client = client
        self.scripts = self._load_scripts()

    def _load_scripts(self):
        scripts_path = Path(__file__).parents[0].joinpath('scripts')
        scripts = {}
        for script_file in scripts_path.glob('*.lua'):
            with open(script_file, 'r') as f:
                scripts[script_file.stem] = self.client.register_script(f.read())
        return scripts

    def lock(self, key, value, timeout=1000) -> bool:
        return bool(self.scripts['lock'](keys=[key, value, timeout]))

    def unlock(self, key, value) -> bool:
        return bool(self.scripts['unlock'](keys=[key, value]))

    @contextmanager
    def __call__(self, key, value, timeout=1000):
        self.lock(key, value, timeout)
        try:
            yield
        finally:
            self.unlock(key, value)
