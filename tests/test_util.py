from transponster.util import WrappedQueue, ClosedException
from pytest import mark as m, raises

class TestWrappedQueue:
    def test_empty_closed(self):

        queue = WrappedQueue()
        queue.close()
        with raises(ClosedException):
            queue.get()

    def test_not_empty_closed(self):

        queue = WrappedQueue()
        queue.put("Hello")
        queue.close()
        assert queue.get() == "Hello"
        with raises(ClosedException):
            queue.get()

    def test_fail_put_to_closed_queue(self):

        queue = WrappedQueue()
        queue.close()
        with raises(ClosedException):
            queue.put("Fail")