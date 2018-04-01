import cherrypy

from multiprocessing import Process
from threading import Lock
from typing import Callable, MutableMapping

OnMissFunc = Callable[[str], object]


@cherrypy.expose
class Node(object):
    _cache = None  # type: MutableMapping
    _lock = None   # type: Lock

    def __init__(self, on_miss: OnMissFunc):
        self.on_miss = on_miss
        self._cache = {}
        self._lock = Lock()

    @cherrypy.popargs('key')
    @cherrypy.tools.json_out()
    def GET(self, key: str) -> object:
        with self._lock:
            value = self._cache.get(key)
            
        if value is None and self.on_miss is not None:
            value = self.on_miss(key)
            if value is not None:
                with self._lock:
                    self._cache[key] = value

        if value is None:
            raise cherrypy.HTTPError(404)

        return value

    @cherrypy.popargs('key')
    @cherrypy.tools.json_in()
    def POST(self, key: str):
        with self._lock:
            self._cache[key] = cherrypy.request.json

    @cherrypy.popargs('key')
    def DELETE(self, key: str) -> bool:
        with cherrypy.HTTPError.handle(KeyError, 404):
            with self._lock:
                del self._cache[key]


class Coordinator(object):

    def __init__(self, port: int, num_nodes: int):
        self.port = port
        self.num_nodes = num_nodes

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def info(self):
        return {'nodes': self.num_nodes, 'start_port': self.port + 1}


def error_page_404(status, message, traceback, version):
    return ''


def _start_node(port: int, on_miss: OnMissFunc):
    cherrypy.config.update({
        'global': {
            'environment': 'production',
            'server.socket_host': '127.0.0.1',
            'server.socket_port': port
        }
    })
    
    cherrypy.tree.mount(Node(on_miss), '/',
        {'/':
            {'request.dispatch': cherrypy.dispatch.MethodDispatcher()}
        }
    )

    cherrypy.config.update({'error_page.404': error_page_404})
    cherrypy.engine.start()
    cherrypy.engine.block()


def start_nodes(start_port: int, num_nodes: int, on_miss: OnMissFunc=None):
    procs = []
    for i in range(num_nodes):
        p = Process(target=_start_node, args=(start_port + i, on_miss))
        p.start()
        procs.append(p)

    return procs


def run(port: int=5555, num_nodes: int=1, on_miss: OnMissFunc=None):
    procs = start_nodes(port + 1, num_nodes, on_miss)

    cherrypy.config.update({
        'global': {
            'environment': 'production',
            'server.socket_host': '127.0.0.1',
            'server.socket_port': port
        }
    })
    
    cherrypy.tree.mount(Coordinator(port, num_nodes), '/')
    cherrypy.engine.start()
    cherrypy.engine.block()

    for p in procs:
        p.join()


if __name__ == '__main__':
    # Just for testing
    run(num_nodes=2)

