from granian import Granian
from granian.constants import Loops, Interfaces


if __name__ == "__main__":
    Granian(
        target="app.main:app",
        address="127.0.0.1",
        port=8000,
        workers=1,
        loop=Loops.asyncio,
        log_enabled=True,
        interface=Interfaces.ASGI,
        reload=True
    ).serve()