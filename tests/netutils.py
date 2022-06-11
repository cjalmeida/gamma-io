import socket


def random_port():
    """Return a free random port."""
    sock = socket.socket()
    sock.bind(("", 0))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    port = sock.getsockname()[1]
    return port
