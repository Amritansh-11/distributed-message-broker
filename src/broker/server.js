import net from 'net';
import { ProtocolParser } from '../protocol/parser.js';
import { COMMAND_TYPES, CommandFormatter } from '../protocol/commands.js';
import { MessageBroker } from './pubsub_broker.js';

export class Server {
  /**
   * @param {object} options 
   * @param {number} [options.port=4222]
   * @param {string} [options.host='127.0.0.1']
   */
  constructor(options = {}) {
    this.port = options.port || 4222;
    this.host = options.host || '127.0.0.1';
    this.broker = new MessageBroker();
    this.tcpServer = null;
  }

  start() {
    return new Promise((resolve, reject) => {
      this.tcpServer = net.createServer((socket) => {
        this._handleConnection(socket);
      });

      this.tcpServer.on('error', (err) => {
        console.error('[Server Error]', err);
        reject(err);
      });

      this.tcpServer.listen(this.port, this.host, () => {
        console.log(`[Broker Server] Listening on tcp://${this.host}:${this.port}`);
        resolve();
      });
    });
  }

  stop() {
    return new Promise((resolve) => {
      if (this.tcpServer) {
        this.tcpServer.close(() => {
          console.log('[Broker Server] Server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  _handleConnection(socket) {
    const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`[Client Connected] ${clientAddr}`);

    const parser = new ProtocolParser();

    socket.on('data', (chunk) => {
      parser.feed(chunk);
    });

    parser.on('command', (cmd) => {
      this._executeCommand(socket, cmd);
    });

    parser.on('error', (err) => {
      console.warn(`[Protocol Error] ${clientAddr}: ${err.message}`);
      if (socket.writable) {
        socket.write(CommandFormatter.error(err.message));
      }
    });

    const cleanup = () => {
      console.log(`[Client Disconnected] ${clientAddr}`);
      this.broker.removeClient(socket);
    };

    socket.on('close', cleanup);
    socket.on('error', (err) => {
      console.error(`[Socket Error] ${clientAddr}: ${err.message}`);
      cleanup();
    });
  }

  _executeCommand(socket, cmd) {
    switch (cmd.type) {
      case COMMAND_TYPES.SUB: {
        this.broker.subscribe(cmd.topic, socket);
        socket.write(CommandFormatter.ok());
        break;
      }

      case COMMAND_TYPES.UNSUB: {
        this.broker.unsubscribe(cmd.topic, socket);
        socket.write(CommandFormatter.ok());
        break;
      }

      case COMMAND_TYPES.PUB: {
        const receivers = this.broker.publish(cmd.topic, cmd.payload);
        socket.write(CommandFormatter.ok());
        break;
      }

      default: {
        socket.write(CommandFormatter.error(`Unsupported command type: ${cmd.type}`));
        break;
      }
    }
  }
}
