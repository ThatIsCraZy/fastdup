import "@testing-library/jest-dom/vitest";

class EventSourceStub extends EventTarget {
  onerror: ((event: Event) => void) | null = null;
  close() {}
}

Object.defineProperty(globalThis, "EventSource", {
  value: EventSourceStub,
  writable: true,
});
