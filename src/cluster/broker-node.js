/**
 * BrokerNode — Entity representing a Broker Instance in the Cluster
 * 
 * Tracks remote broker metadata, health status ('alive' | 'down' | 'unknown'),
 * last successful heartbeat timestamp, and active TCP socket handle.
 */

export class BrokerNode {
  /**
   * @param {object} config 
   * @param {string} config.id - Unique Broker ID (e.g. "broker-1")
   * @param {string} config.host - IP address / host
   * @param {number} config.port - TCP listening port
   * @param {string} [config.status='unknown'] - Initial node status
   */
  constructor({ id, host, port, status = 'unknown' }) {
    this.id = id;
    this.host = host;
    this.port = Number(port);
    /** @type {'alive' | 'down' | 'unknown'} */
    this.status = status;
    /** @type {number | null} Timestamp of last successful heartbeat */
    this.lastHeartbeat = null;
    /** @type {import('net').Socket | null} Active inter-broker TCP socket */
    this.socket = null;
  }

  /**
   * Marks node status as 'alive' and updates heartbeat timestamp.
   */
  markAlive() {
    this.status = 'alive';
    this.lastHeartbeat = Date.now();
  }

  /**
   * Marks node status as 'down' and clears active socket reference.
   */
  markDown() {
    this.status = 'down';
    if (this.socket) {
      try {
        this.socket.destroy();
      } catch (err) {
        // Ignore socket destroy error
      }
      this.socket = null;
    }
  }

  /**
   * Updates heartbeat timestamp and ensures node is marked 'alive'.
   */
  updateHeartbeat() {
    this.lastHeartbeat = Date.now();
    this.status = 'alive';
  }

  /**
   * Serializes broker node state for cluster info responses.
   * @returns {{ id: string, host: string, port: number, status: string }}
   */
  toJSON() {
    return {
      id: this.id,
      host: this.host,
      port: this.port,
      status: this.status
    };
  }
}
