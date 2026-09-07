/**
 * ManagementServer — HTTP Observability & Management Endpoint Server
 * 
 * Exposes HTTP GET endpoints for:
 * - /metrics: Machine-readable system operational metrics
 * - /health: Broker operational status, uptime, and cluster size
 * - /cluster: Known cluster broker topology and status listing
 */

import http from 'http';
import { metricsCollector } from '../metrics/metrics-collector.js';

export class ManagementServer {
  /**
   * @param {object} options
   * @param {number} [options.port=8000] - HTTP port
   * @param {string} [options.host='0.0.0.0'] - HTTP listen host
   * @param {string} [options.brokerId='broker-1'] - Local broker ID
   * @param {import('../cluster/cluster-manager.js').ClusterManager} [options.clusterManager]
   * @param {import('../broker/broker.js').MessageBroker} [options.broker]
   */
  constructor(options = {}) {
    this.port = Number(options.port) || 8000;
    this.host = options.host || '0.0.0.0';
    this.brokerId = options.brokerId || 'broker-1';
    this.clusterManager = options.clusterManager || null;
    this.broker = options.broker || null;
    this.startTime = Date.now();
    this.server = null;
  }

  /**
   * Starts the HTTP management server.
   * @returns {Promise<void>}
   */
  start() {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this._handleRequest(req, res));

      this.server.on('error', (err) => {
        console.error(`[ManagementServer Error] ${err.message}`);
        reject(err);
      });

      this.server.listen(this.port, this.host, () => {
        console.log(`[ManagementServer] Observability HTTP server listening on http://${this.host}:${this.port}`);
        resolve();
      });
    });
  }

  /**
   * Handles incoming HTTP requests.
   * @param {http.IncomingMessage} req 
   * @param {http.ServerResponse} res 
   */
  _handleRequest(req, res) {
    const url = req.url ? req.url.split('?')[0] : '/';
    const method = req.method ? req.method.toUpperCase() : 'GET';

    if (method !== 'GET') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method Not Allowed' }));
      return;
    }

    if (url === '/metrics') {
      const formattedMetrics = metricsCollector.getMetricsFormatted();
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(formattedMetrics);
      return;
    }

    if (url === '/health') {
      const cm = this.clusterManager || (this.broker ? this.broker.clusterManager : null);
      const clusterSize = cm ? cm.nodes.size : 1;
      const uptimeSec = Number(((Date.now() - this.startTime) / 1000).toFixed(2));

      const payload = {
        status: 'HEALTHY',
        brokerId: this.brokerId,
        uptime: uptimeSec,
        clusterSize
      };

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload, null, 2));
      return;
    }

    if (url === '/cluster') {
      const cm = this.clusterManager || (this.broker ? this.broker.clusterManager : null);
      const clusterInfo = cm ? cm.getClusterInfo() : [
        { id: this.brokerId, host: '127.0.0.1', port: 5000, status: 'alive' }
      ];

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(clusterInfo, null, 2));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  }

  /**
   * Gracefully stops the HTTP management server.
   * @returns {Promise<void>}
   */
  stop() {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(() => {
          console.log(`[ManagementServer] Observability HTTP server shut down`);
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
