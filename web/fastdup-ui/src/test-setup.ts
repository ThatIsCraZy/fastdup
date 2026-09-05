import "@testing-library/jest-dom/vitest";

class EventSourceStub extends EventTarget {
  onerror: ((event: Event) => void) | null = null;
  close() {}
}

Object.defineProperty(globalThis, "EventSource", {
  value: EventSourceStub,
  writable: true,
});

// jsdom does not implement native dialog methods; real focus behavior is browser-tested.
HTMLDialogElement.prototype.showModal = function () { this.setAttribute("open", ""); };
HTMLDialogElement.prototype.close = function () { this.removeAttribute("open"); };
