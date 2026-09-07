/**
 * ManagementServer — HTTP Observability & Management Endpoint Server
 * 
 * Exposes HTTP GET endpoints for:
 * - /metrics: Machine-readable system operational metrics
 * - /health: Broker operational status, uptime, and cluster size
 * - /cluster: Known cluster broker topology and status listing
 */

import http from 'http';
import { URL } from 'url';
import { metricsCollector } from '../metrics/metrics-collector.js';
import { ProtocolRequest } from '../protocol/types.js';

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
   * Helper to parse JSON request body
   * @param {http.IncomingMessage} req
   * @returns {Promise<object>}
   */
  _parseJsonBody(req) {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', chunk => {
        body += chunk.toString();
        if (body.length > 1e6) { // 1MB protection
          req.destroy();
          reject(new Error('Payload Too Large'));
        }
      });
      req.on('end', () => {
        if (!body) return resolve({});
        try {
          resolve(JSON.parse(body));
        } catch (err) {
          reject(new Error('Invalid JSON payload'));
        }
      });
      req.on('error', reject);
    });
  }

  /**
   * Handles incoming HTTP requests.
   * @param {http.IncomingMessage} req 
   * @param {http.ServerResponse} res 
   */
  async _handleRequest(req, res) {
    const parsedUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const url = parsedUrl.pathname;
    const method = req.method ? req.method.toUpperCase() : 'GET';

    if (url === '/metrics' && method === 'GET') {
      const formattedMetrics = metricsCollector.getMetricsFormatted();
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(formattedMetrics);
      return;
    }

    if (url === '/health' && method === 'GET') {
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

    if (url === '/cluster' && method === 'GET') {
      const cm = this.clusterManager || (this.broker ? this.broker.clusterManager : null);
      const clusterInfo = cm ? cm.getClusterInfo() : [
        { id: this.brokerId, host: '127.0.0.1', port: 5000, status: 'alive' }
      ];

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(clusterInfo, null, 2));
      return;
    }

    if (url === '/topic' && method === 'POST') {
      if (!this.broker) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Broker engine unavailable' }));
        return;
      }
      try {
        const body = await this._parseJsonBody(req);
        const { topic, partitions = 3, replicationFactor = 1 } = body;
        if (!topic) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing required field: topic' }));
          return;
        }
        const resp = await this.broker.handleRequest(ProtocolRequest.createTopic(topic, Number(partitions), Number(replicationFactor)));
        res.writeHead(resp.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(resp));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    if (url === '/produce' && method === 'POST') {
      if (!this.broker) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Broker engine unavailable' }));
        return;
      }
      try {
        const body = await this._parseJsonBody(req);
        const { topic, message, partition, key } = body;
        if (!topic || message === undefined) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing required fields: topic, message' }));
          return;
        }
        const startTime = Date.now();
        const resp = await this.broker.handleRequest(ProtocolRequest.produce(
          topic,
          message,
          partition !== undefined ? Number(partition) : undefined,
          key
        ));
        const duration = Date.now() - startTime;

        if (resp.success) {
          metricsCollector.increment('messages_produced_total');
          metricsCollector.recordLatency('produce', duration);
        } else {
          metricsCollector.increment('produce_errors_total');
        }

        res.writeHead(resp.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(resp));
      } catch (err) {
        metricsCollector.increment('produce_errors_total');
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    if (url === '/consume' && method === 'GET') {
      if (!this.broker) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Broker engine unavailable' }));
        return;
      }
      try {
        const topic = parsedUrl.searchParams.get('topic');
        const partitionStr = parsedUrl.searchParams.get('partition');
        const offsetStr = parsedUrl.searchParams.get('offset');
        const groupId = parsedUrl.searchParams.get('groupId');
        const consumerId = parsedUrl.searchParams.get('consumerId');

        if (!topic) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing required parameter: topic' }));
          return;
        }

        const partition = partitionStr !== null ? Number(partitionStr) : undefined;
        const offset = offsetStr !== null ? Number(offsetStr) : undefined;

        const startTime = Date.now();
        const resp = await this.broker.handleRequest(ProtocolRequest.consume(
          topic,
          partition,
          offset,
          groupId,
          consumerId
        ));
        const duration = Date.now() - startTime;

        if (resp.success) {
          metricsCollector.increment('messages_consumed_total');
          metricsCollector.recordLatency('consume', duration);
        } else {
          metricsCollector.increment('consume_errors_total');
        }

        res.writeHead(resp.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(resp));
      } catch (err) {
        metricsCollector.increment('consume_errors_total');
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
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

