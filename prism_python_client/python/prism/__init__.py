from threading import Thread, Event
from typing import List
from prism._prism import Client as RustClient, Wavelet, Photon

DEFAULT_PING_RATE = 50


def ping_loop(client, rate, event):
    while True:
        if event.wait(rate):
            break
        client.ping()


class Client:
    def __init__(self, addr, callable, ping_rate=DEFAULT_PING_RATE):
        self._client = RustClient(addr, callable)
        self._event = Event()
        self._thread = Thread(
            target=ping_loop, args=(self._client, ping_rate, self._event)
        )
        self._thread.start()

    def __del__(self):
        self._event.set()

    def add_beam(self, beam: str):
        self._client.add_beam(beam)

    def transmissions(self) -> List[str]:
        self._client.transmissions()

    def subscribe(self, beam: str, index: int | None = None):
        self._client.subscribe(beam, index)

    def unsubscribe(self, beam: str):
        self._client.unsubscribe(beam)

    def emit(self, beam: str, payload: bytes):
        self._client.emit(beam, payload)


__all__ = [Client, Wavelet, Photon]
