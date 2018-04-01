import requests


class Client(object):
    num_nodes = None    # type: int
    nodes = None        # type: list(str)

    def __init__(self, host: str='127.0.0.1', port: int=5555, https: bool=False):
        self.host = 'https://{}'.format(host) if https else 'http://{}'.format(host)
        self.port = port
        self.coord = '{}:{}'.format(self.host, self.port)

    def connect(self):
        resp = requests.get('{}/info'.format(self.coord))
        assert resp.status_code == 200
        info = resp.json()
        self.num_nodes = info['nodes']
        self.nodes = ['{}:{}'.format(self.host, info['start_port'] + i)
                      for i in range(self.num_nodes)]

    def _key_url(self, key: str):
        # TODO: consistent hashing
        idx = hash(key) % self.num_nodes
        return '{}/{}'.format(self.nodes[idx], key)

    def get(self, key: str):
        resp = requests.get(self._key_url(key))
        if resp.status_code == 200:
            return resp.json()

        return None

    def put(self, key: str, value):
        resp = requests.post(self._key_url(key), json=value)
        assert resp.status_code == 200

    def delete(self, key: str):
        resp = requests.delete(self._key_url(key))
        return resp.status_code == 200

