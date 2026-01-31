import * as k8s from '@kubernetes/client-node';
import yaml from 'js-yaml';

const TOKEN = process.env.KUBEFORGE_TOKEN;
const API_URL = process.env.KUBEFORGE_API_URL;
const CLUSTER_NAME = process.env.CLUSTER_NAME || 'my-cluster';

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
      const dryRun = cmd.payload.dryRun === true;
      const results: Array<{ resource: string; success: boolean; error?: string; log?: string; diff?: string[] }> = [];
      const client = k8s.KubernetesObjectApi.makeApiClient(kc);
      const logs: string[] = [];
      const mode = dryRun ? 'Dry run' : 'Deploying';
      logs.push(`${mode}: ${manifests.length} resource(s) to namespace "${namespace}"`);
      logs.push('');

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
          const dryRunParam = dryRun ? 'All' : undefined;

          // Read current live state
          let liveObj: Record<string, unknown> | null = null;
          try {
            const live = await client.read(resource);
            liveObj = live as unknown as Record<string, unknown>;
          } catch {
            // resource doesn't exist
          }

          if (liveObj) {
            await client.patch(resource, undefined, dryRunParam);
            const action = dryRun ? `${resourceLabel} validated (would update)` : `${resourceLabel} configured`;
            logs.push(`  ✓ ${action}`);

            // Compute field-level diff for dry runs
            const diffLines: string[] = [];
            if (dryRun) {
              const cleanedLive = cleanForDiff(liveObj);
              const cleanedDesired = cleanForDiff(obj);
              deepDiff(cleanedLive, cleanedDesired, '', diffLines);
              if (diffLines.length === 0) {
                logs.push('      (no changes)');
              } else {
                for (const line of diffLines) {
                  logs.push(`      ${line}`);
                }
              }
            }

            results.push({ resource: resourceLabel, success: true, log: action, diff: diffLines });
          } else {
            await client.create(resource, undefined, dryRunParam);
            const action = dryRun ? `${resourceLabel} validated (would create)` : `${resourceLabel} created`;
            logs.push(`  ✓ ${action}`);

            // For new resources, summarize what would be created
            const diffLines: string[] = [];
            if (dryRun) {
              const cleaned = cleanForDiff(obj);
              summarizeObject(cleaned, '', diffLines);
              for (const line of diffLines) {
                logs.push(`      ${line}`);
              }
            }

            results.push({ resource: resourceLabel, success: true, log: action, diff: diffLines });
          }
          logs.push('');
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logs.push(`  ✗ ${resourceLabel} failed: ${msg}`);
          logs.push('');
          results.push({ resource: resourceLabel, success: false, error: msg, log: `${resourceLabel} failed: ${msg}` });
        }
      }

      const succeeded = results.filter(r => r.success).length;
      const failed = results.filter(r => !r.success).length;
      logs.push(`Done: ${succeeded} passed, ${failed} failed`);

      return { result: { results, log: logs.join('\n'), dryRun } };
    }

    case 'get_logs': {
      const namespace = cmd.payload.namespace as string;
      const podName = cmd.payload.podName as string | undefined;
      const search = cmd.payload.search as string | undefined;
      const tailLines = (cmd.payload.tailLines as number) || 200;

      // If a specific pod is given, fetch its logs
      if (podName) {
        const log = await coreApi.readNamespacedPodLog({
          name: podName,
          namespace,
          tailLines,
        });
        let lines = typeof log === 'string' ? log : '';
        if (search) {
          lines = lines.split('\n').filter(l => l.toLowerCase().includes(search.toLowerCase())).join('\n');
        }
        return { result: { pods: [{ name: podName, logs: lines }] } };
      }

      // Otherwise fetch logs for all pods in namespace
      const podList = await coreApi.listNamespacedPod({ namespace });
      const pods: Array<{ name: string; logs: string }> = [];
      for (const pod of (podList.items || []).slice(0, 50)) {
        const name = pod.metadata?.name || '';
        if (!name) continue;
        try {
          const log = await coreApi.readNamespacedPodLog({
            name,
            namespace,
            tailLines: Math.min(tailLines, 100),
          });
          let lines = typeof log === 'string' ? log : '';
          if (search) {
            lines = lines.split('\n').filter(l => l.toLowerCase().includes(search.toLowerCase())).join('\n');
          }
          if (lines.trim()) {
            pods.push({ name, logs: lines });
          }
        } catch {
          // Pod may not have logs (e.g. pending)
        }
      }
      return { result: { pods } };
    }

    default:
      return { error: `Unknown command type: ${cmd.type}` };
  }
}

// Strip server-managed fields for clean diffing
function cleanForDiff(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(cleanForDiff);
  if (obj && typeof obj === 'object') {
    const o = obj as Record<string, unknown>;
    const cleaned: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(o)) {
      // Skip server-managed metadata fields
      if (k === 'managedFields' || k === 'resourceVersion' || k === 'uid' ||
          k === 'creationTimestamp' || k === 'generation' || k === 'selfLink' ||
          k === 'status') continue;
      // Skip kubectl annotations
      if (k === 'annotations' && typeof v === 'object' && v) {
        const annotations = { ...(v as Record<string, unknown>) };
        delete annotations['kubectl.kubernetes.io/last-applied-configuration'];
        delete annotations['deployment.kubernetes.io/revision'];
        if (Object.keys(annotations).length > 0) {
          cleaned[k] = annotations;
        }
        continue;
      }
      if (v === null || v === undefined) continue;
      cleaned[k] = cleanForDiff(v);
    }
    return cleaned;
  }
  return obj;
}

// Format a value for display (compact)
function formatValue(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'string') {
    if (v.length > 80) return `"${v.substring(0, 77)}..."`;
    return `"${v}"`;
  }
  if (typeof v === 'boolean' || typeof v === 'number') return String(v);
  if (Array.isArray(v)) {
    if (v.length === 0) return '[]';
    const items = v.map(formatValue);
    const joined = `[${items.join(', ')}]`;
    return joined.length > 80 ? `[${v.length} items]` : joined;
  }
  if (typeof v === 'object') {
    const keys = Object.keys(v as object);
    if (keys.length === 0) return '{}';
    if (keys.length <= 3) {
      const pairs = keys.map(k => `${k}: ${formatValue((v as Record<string, unknown>)[k])}`);
      const joined = `{${pairs.join(', ')}}`;
      if (joined.length <= 80) return joined;
    }
    return `{${keys.length} fields}`;
  }
  return String(v);
}

// Deep diff two objects, producing human-readable change lines
function deepDiff(live: unknown, desired: unknown, path: string, out: string[]): void {
  if (live === desired) return;

  // Both are primitives or different types
  if (typeof live !== typeof desired || live === null || desired === null ||
      typeof live !== 'object' || typeof desired !== 'object') {
    if (live !== desired) {
      out.push(`~ ${path}: ${formatValue(live)} → ${formatValue(desired)}`);
    }
    return;
  }

  const liveIsArray = Array.isArray(live);
  const desiredIsArray = Array.isArray(desired);

  // Array diff
  if (liveIsArray && desiredIsArray) {
    const liveArr = live as unknown[];
    const desiredArr = desired as unknown[];
    const maxLen = Math.max(liveArr.length, desiredArr.length);
    for (let i = 0; i < maxLen; i++) {
      const p = `${path}[${i}]`;
      if (i >= liveArr.length) {
        out.push(`+ ${p}: ${formatValue(desiredArr[i])}`);
      } else if (i >= desiredArr.length) {
        out.push(`- ${p}: ${formatValue(liveArr[i])}`);
      } else {
        deepDiff(liveArr[i], desiredArr[i], p, out);
      }
    }
    return;
  }

  // Object diff
  if (!liveIsArray && !desiredIsArray) {
    const liveObj = live as Record<string, unknown>;
    const desiredObj = desired as Record<string, unknown>;
    const allKeys = new Set([...Object.keys(liveObj), ...Object.keys(desiredObj)]);

    for (const key of allKeys) {
      const p = path ? `${path}.${key}` : key;
      if (!(key in liveObj)) {
        out.push(`+ ${p}: ${formatValue(desiredObj[key])}`);
      } else if (!(key in desiredObj)) {
        out.push(`- ${p}: ${formatValue(liveObj[key])}`);
      } else {
        deepDiff(liveObj[key], desiredObj[key], p, out);
      }
    }
    return;
  }

  // Type mismatch (array vs object)
  out.push(`~ ${path}: ${formatValue(live)} → ${formatValue(desired)}`);
}

// Summarize a new object's key fields
function summarizeObject(obj: unknown, path: string, out: string[]): void {
  if (obj === null || obj === undefined) return;
  if (typeof obj !== 'object') {
    out.push(`+ ${path}: ${formatValue(obj)}`);
    return;
  }
  if (Array.isArray(obj)) {
    if (obj.length === 0) return;
    // For arrays, show count and summarize first item
    if (obj.length > 1) {
      out.push(`+ ${path}: [${obj.length} items]`);
    }
    if (typeof obj[0] === 'object' && obj[0]) {
      summarizeObject(obj[0], `${path}[0]`, out);
    } else {
      out.push(`+ ${path}[0]: ${formatValue(obj[0])}`);
    }
    return;
  }
  const o = obj as Record<string, unknown>;
  for (const [k, v] of Object.entries(o)) {
    const p = path ? `${path}.${k}` : k;
    if (v === null || v === undefined) continue;
    if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v as object).length > 0) {
      // Recurse into objects but cap depth
      if (p.split('.').length < 5) {
        summarizeObject(v, p, out);
      } else {
        out.push(`+ ${p}: ${formatValue(v)}`);
      }
    } else {
      out.push(`+ ${p}: ${formatValue(v)}`);
    }
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

  // Long-poll loop — server holds request open until a command is available or ~25s timeout
  console.log('Entering long-poll loop');
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
      // Immediately re-poll — server controls the wait time via long polling
    } catch (err) {
      console.error('Poll error:', err instanceof Error ? err.message : err);
      // Back off on errors to avoid tight retry loops
      await new Promise((r) => setTimeout(r, 3000));
    }
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
