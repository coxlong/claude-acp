package ws

import "testing"

func TestOutboxRequeuePreservesOrder(t *testing.T) {
	o := newOutbox(1024)
	o.push([]byte("a"))
	o.push([]byte("b"))

	frames := o.drain()
	if len(frames) != 2 {
		t.Fatalf("drained %d frames", len(frames))
	}

	// "a" was delivered; "b" was not. New output arrives while requeueing.
	o.push([]byte("c"))
	o.requeue(frames[1:])

	got := o.drain()
	if len(got) != 2 || string(got[0]) != "b" || string(got[1]) != "c" {
		t.Fatalf("got %q", got)
	}
}

func TestOutboxOverflow(t *testing.T) {
	o := newOutbox(3)
	if !o.push([]byte("ab")) {
		t.Fatal("first push rejected")
	}
	if o.push([]byte("cd")) {
		t.Fatal("overflowing push accepted")
	}
}
