package main

import (
	"log"
	"sync"

	"github.com/sireax/Emmet-Go-Server/internal/queue"
)

type writerConfig struct {
	WriteManyFn func(...[]byte) error
	WriteFn     func([]byte) error
}

type writer struct {
	mu       sync.Mutex
	config   writerConfig
	messages queue.Queue
	closed   bool
	done     chan struct{}
}

func newWriter(config writerConfig) *writer {
	w := &writer{
		config:   config,
		messages: queue.New(),
		done:     make(chan struct{}, 1),
	}

	go w.runWriteRoutine()
	return w
}

func (w *writer) runWriteRoutine() {
	for {
		msg, ok := w.messages.Wait()
		if !ok {
			if w.messages.Closed() {
				return
			}
			continue
		}

		messageCount := w.messages.Len()
		if messageCount > 0 {

			messagesCap := messageCount + 1
			msgs := make([][]byte, 0, messagesCap)
			msgs = append(msgs, msg)

			for messageCount > 0 {
				messageCount--
				m, ok := w.messages.Remove()
				if ok {
					msgs = append(msgs, m)
				} else {
					if w.messages.Closed() {
						return
					}
					break
				}
			}
			if len(msgs) > 0 {
				w.mu.Lock()
				if len(msgs) == 1 {
					log.Println(msgs[0])
					w.config.WriteFn(msgs[0])
				} else {
					log.Println(msgs)
					w.config.WriteManyFn(msgs...)
				}
				w.mu.Unlock()
			}
		}
	}
}

func (w *writer) enqueue(data []byte) {
	w.messages.Add(data)
}

func (w *writer) close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	remaining := w.messages.CloseRemaining()
	if len(remaining) > 0 {
		w.mu.Lock()
		w.config.WriteManyFn(remaining...)
		w.mu.Unlock()
	}

	return nil
}
