/**
 * ConfigLoader — Operational Environment Variable Configuration System
 * 
 * Loads operational settings from environment variables with sensible defaults
 * for local development and multi-broker Docker clusters.
 */

export class ConfigLoader {
  /**
   * Parses environment variables and returns a standardized configuration object.
   * @param {object} [env=process.env] - Environment variables object
   * @returns {object} Standardized broker configuration
   */
  static loadConfig(env = process.env) {
    const brokerId = env.BROKER_ID || 'broker-1';
    const host = env.BROKER_HOST || '127.0.0.1';
    const port = Number(env.BROKER_PORT) || 5000;
    const httpPort = Number(env.HTTP_PORT) || Number(env.PORT) || (port + 3000);
    const dataDir = env.BROKER_DATA_DIR || `./data/${brokerId}`;
    const logLevel = (env.LOG_LEVEL || 'INFO').toUpperCase();
    const replicationFactor = Number(env.REPLICATION_FACTOR) || 1;
    const heartbeatIntervalMs = Number(env.HEARTBEAT_INTERVAL) || 1000;
    const heartbeatTimeoutMs = Number(env.ELECTION_TIMEOUT) || 2000;

    const clusterBrokersRaw = env.CLUSTER_BROKERS || `${brokerId}:${host}:${port}`;
    const clusterConfig = ConfigLoader.parseClusterBrokers(clusterBrokersRaw);

    return {
      brokerId,
      host,
      port,
      httpPort,
      dataDir,
      logLevel,
      replicationFactor,
      heartbeatIntervalMs,
      heartbeatTimeoutMs,
      clusterBrokersRaw,
      clusterConfig
    };
  }

  /**
   * Parses cluster broker connection string formatted as:
   * "broker-1:127.0.0.1:5000,broker-2:127.0.0.1:5001,broker-3:127.0.0.1:5002"
   * 
   * @param {string} raw 
   * @returns {Array<{ id: string, host: string, port: number }>}
   */
  static parseClusterBrokers(raw) {
    if (!raw || typeof raw !== 'string') return [];
    
    const parts = raw.split(',').map(s => s.trim()).filter(Boolean);
    const result = [];

    for (const part of parts) {
      const tokens = part.split(':');
      if (tokens.length >= 3) {
        result.push({
          id: tokens[0].trim(),
          host: tokens[1].trim(),
          port: Number(tokens[2].trim()) || 5000
        });
      } else if (tokens.length === 2) {
        result.push({
          id: tokens[0].trim(),
          host: tokens[0].trim(),
          port: Number(tokens[1].trim()) || 5000
        });
      }
    }

    return result;
  }
}
