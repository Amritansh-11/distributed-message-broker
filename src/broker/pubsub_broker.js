import { CommandFormatter } from '../protocol/commands.js';

export class MessageBroker {
  constructor() {
    /** @type {Map<string, Set<import('net').Socket>>} */
    this.topics = new Map();
    this.messageCounter = 0;
  }

  subscribe(topic, socket) {
    if (!this.topics.has(topic)) {
      this.topics.set(topic, new Set());
    }
    this.topics.get(topic).add(socket);
  }

  unsubscribe(topic, socket) {
    if (this.topics.has(topic)) {
      this.topics.get(topic).delete(socket);
      if (this.topics.get(topic).size === 0) {
        this.topics.delete(topic);
      }
    }
  }

  removeClient(socket) {
    for (const [topic, sockets] of this.topics.entries()) {
      sockets.delete(socket);
      if (sockets.size === 0) {
        this.topics.delete(topic);
      }
    }
  }

  publish(topic, payload) {
    const receivers = this.topics.get(topic);
    if (!receivers || receivers.size === 0) {
      return 0;
    }

    this.messageCounter++;
    const messageId = String(this.messageCounter);
    const frame = CommandFormatter.message(topic, messageId, payload);

    let deliveredCount = 0;
    for (const socket of receivers) {
      if (socket.writable) {
        socket.write(frame);
        deliveredCount++;
      }
    }
    return deliveredCount;
  }
}
