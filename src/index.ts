import * as k8s from '@kubernetes/client-node';
import yaml from 'js-yaml';

const TOKEN = process.env.KUBEFORGE_TOKEN;
const API_URL = process.env.KUBEFORGE_API_URL;
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'my-cluster';
const POLL_INTERVAL = 5000;

if (!TOKEN || !API_URL) {
  console.error('KUBEFORGE_TOKEN and KUBEFORGE_API_URL are required');
  process.exit(1);
}

const kc = new k8s.KubeConfig();
try {
  kc.loadFromCluster();
} catch {
  console.log('Not running in-cluster, trying default kubeconfig');
  kc.loadFromDefault();
}

let agentId: string | null = null;
let running = true;

async function register(): Promise<string> {
  // Get cluster version
  let clusterVersion: string | undefined;
  try {
    const versionApi = kc.makeApiClient(k8s.VersionApi);
    const info = await versionApi.getCode();
    clusterVersion = info.gitVersion;
  } catch {
    console.warn('Could not fetch cluster version');
  }

  const res = await fetch(`${API_URL}/api/agent/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token: TOKEN, clusterName: CLUSTER_NAME, clusterVersion }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Registration failed: ${res.status} ${body}`);
  }

  const data = await res.json();
  console.log(`Registered as agent ${data.agentId}, connection ${data.connectionId}`);
  return data.agentId;
}

interface Command {
  id: string;
  type: string;
  payload: Record<string, unknown>;
}

async function poll(): Promise<Command | null> {
  const res = await fetch(`${API_URL}/api/agent/poll?agentId=${agentId}`, {
    headers: { 'Authorization': `Bearer ${TOKEN}` },
  });

  if (!res.ok) {
    console.error(`Poll failed: ${res.status}`);
    return null;
  }

  const data = await res.json();
  return data.command || null;
}

async function submitResult(commandId: string, status: 'completed' | 'failed', result?: unknown, error?: string) {
  await fetch(`${API_URL}/api/agent/result`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${TOKEN}`,
    },
    body: JSON.stringify({ agentId, commandId, status, result, error }),
  });
}

async function executeCommand(cmd: Command): Promise<{ result?: unknown; error?: string }> {
  const coreApi = kc.makeApiClient(k8s.CoreV1Api);
  const appsApi = kc.makeApiClient(k8s.AppsV1Api);
  const batchApi = kc.makeApiClient(k8s.BatchV1Api);
  const networkingApi = kc.makeApiClient(k8s.NetworkingV1Api);
  const autoscalingApi = kc.makeApiClient(k8s.AutoscalingV1Api);

  switch (cmd.type) {
    case 'list_namespaces': {
      const res = await coreApi.listNamespace();
      return { result: (res.items || []).map((ns) => ns.metadata?.name || '').filter(Boolean) };
    }

    case 'list_resources': {
      const namespace = cmd.payload.namespace as string;
      const resources: Array<{ kind: string; apiVersion: string; name: string; namespace: string }> = [];

      const fetchers: Array<() => Promise<void>> = [
        async () => { const r = await appsApi.listNamespacedDeployment({ namespace }); for (const i of r.items || []) resources.push({ kind: 'Deployment', apiVersion: 'apps/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await coreApi.listNamespacedService({ namespace }); for (const i of r.items || []) resources.push({ kind: 'Service', apiVersion: 'v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await coreApi.listNamespacedConfigMap({ namespace }); for (const i of r.items || []) resources.push({ kind: 'ConfigMap', apiVersion: 'v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await coreApi.listNamespacedSecret({ namespace }); for (const i of r.items || []) resources.push({ kind: 'Secret', apiVersion: 'v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await networkingApi.listNamespacedIngress({ namespace }); for (const i of r.items || []) resources.push({ kind: 'Ingress', apiVersion: 'networking.k8s.io/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await appsApi.listNamespacedStatefulSet({ namespace }); for (const i of r.items || []) resources.push({ kind: 'StatefulSet', apiVersion: 'apps/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await appsApi.listNamespacedDaemonSet({ namespace }); for (const i of r.items || []) resources.push({ kind: 'DaemonSet', apiVersion: 'apps/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await batchApi.listNamespacedCronJob({ namespace }); for (const i of r.items || []) resources.push({ kind: 'CronJob', apiVersion: 'batch/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await batchApi.listNamespacedJob({ namespace }); for (const i of r.items || []) resources.push({ kind: 'Job', apiVersion: 'batch/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await autoscalingApi.listNamespacedHorizontalPodAutoscaler({ namespace }); for (const i of r.items || []) resources.push({ kind: 'HorizontalPodAutoscaler', apiVersion: 'autoscaling/v1', name: i.metadata?.name || '', namespace }); },
        async () => { const r = await coreApi.listNamespacedPersistentVolumeClaim({ namespace }); for (const i of r.items || []) resources.push({ kind: 'PersistentVolumeClaim', apiVersion: 'v1', name: i.metadata?.name || '', namespace }); },
      ];

      await Promise.allSettled(fetchers.map((f) => f()));
      return { result: resources };
    }

    case 'get_manifests': {
      const namespace = cmd.payload.namespace as string;
      const requested = cmd.payload.resources as { kind: string; name: string }[];
      const docs: string[] = [];

      for (const { kind, name } of requested) {
        try {
          let raw: Record<string, unknown> | null = null;
          switch (kind) {
            case 'Deployment': raw = await appsApi.readNamespacedDeployment({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'Service': raw = await coreApi.readNamespacedService({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'ConfigMap': raw = await coreApi.readNamespacedConfigMap({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'Secret': raw = await coreApi.readNamespacedSecret({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'Ingress': raw = await networkingApi.readNamespacedIngress({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'StatefulSet': raw = await appsApi.readNamespacedStatefulSet({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'DaemonSet': raw = await appsApi.readNamespacedDaemonSet({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'CronJob': raw = await batchApi.readNamespacedCronJob({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'Job': raw = await batchApi.readNamespacedJob({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'HorizontalPodAutoscaler': raw = await autoscalingApi.readNamespacedHorizontalPodAutoscaler({ name, namespace }) as unknown as Record<string, unknown>; break;
            case 'PersistentVolumeClaim': raw = await coreApi.readNamespacedPersistentVolumeClaim({ name, namespace }) as unknown as Record<string, unknown>; break;
          }
          if (raw) {
            const cleaned = cleanManifest(raw);
            docs.push(yaml.dump(cleaned, { lineWidth: -1 }));
          }
        } catch (err) {
          console.warn(`Failed to fetch ${kind}/${name}:`, err instanceof Error ? err.message : err);
        }
      }

      return { result: docs.join('---\n') };
    }

    case 'apply_manifest': {
      const namespace = cmd.payload.namespace as string;
      const manifests = cmd.payload.manifests as string[];
      const results: Array<{ resource: string; success: boolean; error?: string }> = [];
      const client = k8s.KubernetesObjectApi.makeApiClient(kc);

      for (const manifestStr of manifests) {
        const obj = yaml.load(manifestStr) as Record<string, unknown>;
        const kind = obj.kind as string;
        const metadata = obj.metadata as Record<string, unknown>;
        const name = metadata?.name as string;
        const resourceLabel = `${kind}/${name}`;

        try {
          if (!obj.metadata || typeof obj.metadata !== 'object') {
            obj.metadata = { name: name || 'unnamed' };
          }
          if (!(obj.metadata as Record<string, unknown>).namespace) {
            (obj.metadata as Record<string, unknown>).namespace = namespace;
          }

          const resource = obj as unknown as k8s.KubernetesObject & { metadata: { name: string } };
          let exists = false;
          try {
            await client.read(resource);
            exists = true;
          } catch {
            // resource doesn't exist
          }
          if (exists) {
            await client.patch(resource);
          } else {
            await client.create(resource);
          }
          results.push({ resource: resourceLabel, success: true });
        } catch (err) {
          results.push({ resource: resourceLabel, success: false, error: err instanceof Error ? err.message : String(err) });
        }
      }

      return { result: results };
    }

    default:
      return { error: `Unknown command type: ${cmd.type}` };
  }
}

function cleanManifest(obj: Record<string, unknown>): Record<string, unknown> {
  const cleaned = { ...obj };
  delete cleaned.status;
  if (cleaned.metadata && typeof cleaned.metadata === 'object') {
    const meta = { ...(cleaned.metadata as Record<string, unknown>) };
    delete meta.managedFields;
    delete meta.resourceVersion;
    delete meta.uid;
    delete meta.creationTimestamp;
    delete meta.generation;
    delete meta.selfLink;
    if (meta.annotations && Object.keys(meta.annotations as object).length === 0) delete meta.annotations;
    if (meta.labels && Object.keys(meta.labels as object).length === 0) delete meta.labels;
    cleaned.metadata = meta;
  }
  return cleaned;
}

async function main() {
  console.log(`KubeForge Agent starting - cluster: ${CLUSTER_NAME}, API: ${API_URL}`);

  // Register with retry
  while (running && !agentId) {
    try {
      agentId = await register();
    } catch (err) {
      console.error('Registration failed, retrying in 5s:', err instanceof Error ? err.message : err);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }

  // Poll loop
  while (running) {
    try {
      const cmd = await poll();
      if (cmd) {
        console.log(`Executing command ${cmd.id}: ${cmd.type}`);
        try {
          const { result, error } = await executeCommand(cmd);
          if (error) {
            await submitResult(cmd.id, 'failed', undefined, error);
          } else {
            await submitResult(cmd.id, 'completed', result);
          }
          console.log(`Command ${cmd.id} completed`);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.error(`Command ${cmd.id} failed:`, msg);
          await submitResult(cmd.id, 'failed', undefined, msg);
        }
      }
    } catch (err) {
      console.error('Poll error:', err instanceof Error ? err.message : err);
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL));
  }
}

process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down');
  running = false;
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down');
  running = false;
});

main().catch((err) => {
  console.error('Agent fatal error:', err);
  process.exit(1);
});
