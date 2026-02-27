import { useState, useCallback, useEffect } from 'react';
import axios from 'axios';
import CodeMirror from '@uiw/react-codemirror';
import { yaml } from '@codemirror/lang-yaml';
import { Play, Loader2, Database, FileJson, FileText, Clock, AlertCircle, Settings, X, CheckCircle2, Download, Eraser } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = 'http://localhost:8000';

const DEFAULT_SCHEMA = `project: supply_chain_data_generator
version: "1.0.0"
temporal:
  start_date: "2023-01-01"
  end_date: "2024-12-31"
global_messiness:
  null_pct: 0.04
fk_cache:
  enabled: true
database:
  entities:
    - name: warehouses
      row_count: 50
      columns:
        - name: warehouse_id
          type: uuid
          primary_key: true
        - name: created_at
          type: timestamp
          temporal: true
file_sources: []
api_dumps: []
`;

export default function App() {
    const [schemaText, setSchemaText] = useState(DEFAULT_SCHEMA);
    const [status, setStatus] = useState<'idle' | 'running' | 'completed' | 'failed'>('idle');
    const [jobId, setJobId] = useState<string | null>(null);
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    // Connection String State
    const [isSettingsOpen, setIsSettingsOpen] = useState(false);
    const [connectionString, setConnectionString] = useState('');
    const [testDbStatus, setTestDbStatus] = useState<'idle' | 'testing' | 'success' | 'failed'>('idle');
    const [testDbMessage, setTestDbMessage] = useState('');

    const pollStatus = useCallback(async (currentJobId: string) => {
        try {
            const res = await axios.get(`${API_BASE}/status/${currentJobId}`);
            if (res.data.status === 'completed') {
                const resultRes = await axios.get(`${API_BASE}/result/${currentJobId}`);
                setResult(resultRes.data.result);
                setStatus('completed');
            } else if (res.data.status === 'failed') {
                const resultRes = await axios.get(`${API_BASE}/result/${currentJobId}`);
                setError(resultRes.data.error || 'Job failed');
                setStatus('failed');
            } else {
                setTimeout(() => pollStatus(currentJobId), 2000);
            }
        } catch (err: any) {
            setError(err.message || 'Error polling status');
            setStatus('failed');
        }
    }, []);

    const handleTestConnection = async () => {
        if (!connectionString) return;
        try {
            setTestDbStatus('testing');
            setTestDbMessage('');
            const res = await axios.post(`${API_BASE}/test-connection`, {
                connection_string: connectionString
            });
            if (res.data.status === 'success') {
                setTestDbStatus('success');
                setTestDbMessage('Connection successful!');
            } else {
                setTestDbStatus('failed');
                setTestDbMessage(res.data.message || 'Connection failed');
            }
        } catch (err: any) {
            setTestDbStatus('failed');
            setTestDbMessage(err.response?.data?.detail || err.message);
        }
    };

    const handleDownload = (file: any) => {
        // file.filename is the relative path like output_<jobId>/folder/file.csv
        // Normalize backslashes to forward slashes
        const relPath = (file.filename as string).replace(/\\/g, '/');
        const url = `${API_BASE}/download?job_id=${encodeURIComponent(jobId!)}&path=${encodeURIComponent(relPath)}`;
        const a = document.createElement('a');
        a.href = url;
        a.download = relPath.split('/').pop() || 'download';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    };

    const handleMockMe = async () => {
        try {
            setStatus('running');
            setError(null);
            setResult(null);

            const payload = {
                schema: schemaText,
                connection_string: connectionString || undefined
            };

            const res = await axios.post(`${API_BASE}/generate`, payload, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            setJobId(res.data.job_id);
            pollStatus(res.data.job_id);
        } catch (err: any) {
            setError(err.response?.data?.detail || err.message);
            setStatus('failed');
        }
    };

    return (
        <div className="min-h-screen bg-slate-50 text-slate-900 font-sans p-6 md:p-12">
            <div className="max-w-6xl mx-auto space-y-8">
                <header className="flex items-center justify-between border-b border-slate-200 pb-6">
                    <div>
                        <h1 className="text-4xl font-extrabold tracking-tight text-slate-900">
                            Data Forge <span className="text-violet-600">Platform</span>
                        </h1>
                        <p className="text-slate-500 mt-2 text-lg">
                            Schema-driven realistic mock data generation at scale.
                        </p>
                    </div>
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setIsSettingsOpen(true)}
                            className="flex items-center text-slate-600 hover:text-slate-900 transition-colors px-4 py-2 rounded-lg hover:bg-slate-100 font-medium"
                        >
                            <Settings className="w-5 h-5 mr-2" />
                            DB Settings
                        </button>
                        <button
                            onClick={handleMockMe}
                            disabled={status === 'running'}
                            className="group relative inline-flex items-center justify-center px-8 py-3 font-bold text-white transition-all duration-200 bg-violet-600 font-pj rounded-xl hover:bg-violet-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-violet-600 disabled:opacity-70 disabled:cursor-not-allowed"
                        >
                            {status === 'running' ? (
                                <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                            ) : (
                                <Play className="w-5 h-5 mr-2 group-hover:scale-110 transition-transform" />
                            )}
                            {status === 'running' ? 'Generating...' : 'Mock Me'}
                        </button>
                    </div>
                </header>

                {isSettingsOpen && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-sm px-4">
                        <motion.div
                            initial={{ opacity: 0, scale: 0.95 }}
                            animate={{ opacity: 1, scale: 1 }}
                            className="bg-white rounded-2xl shadow-xl w-full max-w-2xl overflow-hidden"
                        >
                            <div className="flex justify-between items-center px-6 py-4 border-b border-slate-100 bg-slate-50">
                                <h3 className="text-lg font-bold text-slate-800 flex items-center">
                                    <Database className="w-5 h-5 mr-2 text-violet-600" />
                                    Database Connection Settings
                                </h3>
                                <button onClick={() => setIsSettingsOpen(false)} className="text-slate-400 hover:text-slate-600">
                                    <X className="w-5 h-5" />
                                </button>
                            </div>

                            <div className="p-6 space-y-4">
                                <p className="text-sm text-slate-600">
                                    Override the backend's default `.env` Database URL. This allows you to push mock data to local instances or Cloud providers like Neon DB.
                                </p>

                                <div>
                                    <label className="block text-sm font-semibold text-slate-700 mb-1">Connection String URI</label>
                                    <input
                                        type="text"
                                        value={connectionString}
                                        onChange={(e) => {
                                            setConnectionString(e.target.value);
                                            setTestDbStatus('idle');
                                        }}
                                        placeholder="postgresql://user:password@localhost:5432/dbname"
                                        className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-violet-500 focus:border-violet-500 outline-none transition-shadow font-mono text-sm"
                                    />
                                </div>

                                <div className="flex gap-2">
                                    <button
                                        onClick={() => setConnectionString('postgresql://postgres:password@localhost:5432/test_01')}
                                        className="text-xs px-3 py-1 bg-slate-100 text-slate-700 rounded hover:bg-slate-200 transition-colors"
                                    >
                                        Local Template
                                    </button>
                                    <button
                                        onClick={() => setConnectionString('postgresql://user:password@ep-cool-db.aws.neon.tech/neondb?sslmode=require')}
                                        className="text-xs px-3 py-1 bg-slate-100 text-slate-700 rounded hover:bg-slate-200 transition-colors"
                                    >
                                        Neon DB Template (?sslmode=require)
                                    </button>
                                    <button
                                        onClick={() => setConnectionString('')}
                                        className="text-xs px-3 py-1 bg-red-50 text-red-600 rounded hover:bg-red-100 transition-colors ml-auto"
                                    >
                                        Clear Override
                                    </button>
                                </div>

                                <div className="mt-6 pt-6 border-t border-slate-100 flex items-center justify-between">
                                    <div className="flex items-center">
                                        {testDbStatus === 'testing' && <span className="text-sm flex items-center text-blue-600"><Loader2 className="w-4 h-4 mr-2 animate-spin" /> Testing...</span>}
                                        {testDbStatus === 'success' && <span className="text-sm flex items-center text-emerald-600"><CheckCircle2 className="w-4 h-4 mr-2" /> {testDbMessage}</span>}
                                        {testDbStatus === 'failed' && <span className="text-sm flex items-center text-red-600"><AlertCircle className="w-4 h-4 mr-2" /> {testDbMessage}</span>}
                                    </div>
                                    <div className="flex gap-3">
                                        <button
                                            onClick={handleTestConnection}
                                            disabled={!connectionString || testDbStatus === 'testing'}
                                            className="px-4 py-2 text-sm font-semibold text-violet-700 bg-violet-50 rounded-lg hover:bg-violet-100 disabled:opacity-50 transition-colors"
                                        >
                                            Test Connection
                                        </button>
                                        <button
                                            onClick={() => setIsSettingsOpen(false)}
                                            className="px-4 py-2 text-sm font-bold text-white bg-slate-800 rounded-lg hover:bg-slate-900 transition-colors"
                                        >
                                            Save & Close
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </motion.div>
                    </div>
                )}

                <main className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                    <section className="flex flex-col h-[700px]">
                        <div className="flex items-center justify-between mb-4">
                            <h2 className="text-xl font-bold flex items-center text-slate-800">
                                <FileJson className="w-5 h-5 mr-2 text-violet-500" />
                                YAML Schema
                            </h2>
                            <button
                                onClick={() => setSchemaText('')}
                                title="Clear all YAML content"
                                className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-semibold text-red-600 bg-red-50 rounded-lg hover:bg-red-100 hover:text-red-700 transition-all duration-200 border border-red-200 hover:shadow-sm active:scale-95"
                            >
                                <Eraser className="w-4 h-4" />
                                Clear YAML
                            </button>
                        </div>
                        <div className="flex-1 overflow-hidden rounded-2xl border border-slate-200 shadow-sm bg-white">
                            <CodeMirror
                                value={schemaText}
                                height="100%"
                                extensions={[yaml()]}
                                onChange={(val) => setSchemaText(val)}
                                className="h-full text-base"
                                theme="light"
                            />
                        </div>
                    </section>

                    <section className="flex flex-col h-[700px]">
                        <h2 className="text-xl font-bold mb-4 flex items-center text-slate-800">
                            <Database className="w-5 h-5 mr-2 text-emerald-500" />
                            Generation Results
                        </h2>
                        <div className="flex-1 overflow-y-auto rounded-2xl border border-slate-200 shadow-sm bg-white p-6">
                            <AnimatePresence mode="popLayout">
                                {status === 'idle' && (
                                    <motion.div
                                        initial={{ opacity: 0 }}
                                        animate={{ opacity: 1 }}
                                        exit={{ opacity: 0 }}
                                        key="idle"
                                        className="h-full flex flex-col items-center justify-center text-slate-400"
                                    >
                                        <FileText className="w-16 h-16 mb-4 opacity-50" />
                                        <p className="text-lg">Ready to generate data.</p>
                                        <p className="text-sm">Click "Mock Me" to begin.</p>
                                    </motion.div>
                                )}

                                {status === 'running' && (
                                    <motion.div
                                        initial={{ opacity: 0, scale: 0.95 }}
                                        animate={{ opacity: 1, scale: 1 }}
                                        exit={{ opacity: 0, scale: 0.95 }}
                                        key="running"
                                        className="h-full flex flex-col items-center justify-center text-violet-600"
                                    >
                                        <Loader2 className="w-16 h-16 animate-spin mb-6" />
                                        <h3 className="text-2xl font-bold animate-pulse">Forging Data...</h3>
                                        <p className="text-slate-500 mt-2">This may take a few minutes for large schemas.</p>
                                    </motion.div>
                                )}

                                {status === 'failed' && (
                                    <motion.div
                                        initial={{ opacity: 0, y: 20 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        key="failed"
                                        className="p-6 bg-red-50 text-red-700 rounded-xl border border-red-200"
                                    >
                                        <div className="flex items-center mb-4">
                                            <AlertCircle className="w-6 h-6 mr-2" />
                                            <h3 className="text-xl font-bold">Generation Failed</h3>
                                        </div>
                                        <pre className="whitespace-pre-wrap text-sm font-mono overflow-auto max-h-96">
                                            {error}
                                        </pre>
                                    </motion.div>
                                )}

                                {status === 'completed' && result && (
                                    <motion.div
                                        initial={{ opacity: 0, y: 20 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        key="completed"
                                        className="space-y-8"
                                    >
                                        <div className="grid grid-cols-2 gap-4">
                                            <div className="p-5 bg-gradient-to-br from-violet-50 to-purple-50 rounded-xl border border-violet-100 shadow-sm">
                                                <p className="text-sm font-semibold text-violet-600 uppercase tracking-wider mb-1">Execution Time</p>
                                                <p className="text-3xl font-black text-slate-900 flex items-center">
                                                    <Clock className="w-6 h-6 mr-2 text-violet-400" />
                                                    {result.execution_seconds}s
                                                </p>
                                            </div>
                                            <div className="p-5 bg-gradient-to-br from-emerald-50 to-teal-50 rounded-xl border border-emerald-100 shadow-sm">
                                                <p className="text-sm font-semibold text-emerald-600 uppercase tracking-wider mb-1">Total Records</p>
                                                <p className="text-3xl font-black text-slate-900 flex items-center">
                                                    <Database className="w-6 h-6 mr-2 text-emerald-400" />
                                                    {result.total_records?.toLocaleString()}
                                                </p>
                                            </div>
                                        </div>

                                        {Object.keys(result.database_tables || {}).length > 0 && (
                                            <div>
                                                <h3 className="text-lg font-bold text-slate-800 mb-3 border-b pb-2">Database Tables</h3>
                                                <div className="space-y-2">
                                                    {Object.entries(result.database_tables).map(([table, info]: [string, any]) => (
                                                        <div key={table} className="flex justify-between items-center p-3 rounded-lg bg-slate-50 hover:bg-slate-100 transition-colors">
                                                            <span className="font-medium text-slate-700">{table}</span>
                                                            <span className="bg-white px-3 py-1 rounded-full text-sm font-semibold text-slate-600 border border-slate-200">
                                                                {info.actual_rows?.toLocaleString()} rows
                                                            </span>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}

                                        {result.files_generated?.length > 0 && (
                                            <div>
                                                <h3 className="text-lg font-bold text-slate-800 mb-3 border-b pb-2">Generated Files</h3>
                                                <div className="space-y-3">
                                                    {result.files_generated.map((file: any, i: number) => (
                                                        <div key={i} className="p-4 rounded-lg bg-slate-50 border border-slate-100 flex items-center justify-between gap-4">
                                                            <div className="min-w-0">
                                                                <div className="font-medium text-slate-800 break-all mb-1">{(file.filename as string).replace(/\\/g, '/').split('/').pop()}</div>
                                                                <div className="flex gap-4 text-sm text-slate-500">
                                                                    <span className="flex items-center"><FileText className="w-4 h-4 mr-1" />{file.format.toUpperCase()}</span>
                                                                    <span>{file.rows?.toLocaleString()} rows</span>
                                                                    <span>{file.size_kb} KB</span>
                                                                </div>
                                                            </div>
                                                            <button
                                                                onClick={() => handleDownload(file)}
                                                                title="Download file"
                                                                className="flex-shrink-0 flex items-center gap-1.5 px-3 py-1.5 text-sm font-semibold text-violet-700 bg-violet-50 rounded-lg hover:bg-violet-100 transition-colors border border-violet-200"
                                                            >
                                                                <Download className="w-4 h-4" />
                                                                Download
                                                            </button>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}

                                        {result.api_dumps_generated?.length > 0 && (
                                            <div>
                                                <h3 className="text-lg font-bold text-slate-800 mb-3 border-b pb-2">API Dumps</h3>
                                                <div className="space-y-3">
                                                    {result.api_dumps_generated.map((api: any, i: number) => (
                                                        <div key={i} className="p-4 rounded-lg bg-slate-50 border border-slate-100">
                                                            <div className="flex justify-between items-start gap-4">
                                                                <div>
                                                                    <div className="font-medium text-slate-800">{api.name}</div>
                                                                    <div className="text-sm text-slate-500 mt-1">{api.pages} pages</div>
                                                                    <div className="mt-2">
                                                                        <code className="text-xs bg-slate-200 text-slate-600 px-2 py-1 rounded font-mono break-all">
                                                                            {API_BASE}/api-data/{jobId}/{api.name}?page=1
                                                                        </code>
                                                                    </div>
                                                                </div>
                                                                <div className="text-right flex-shrink-0">
                                                                    <div className="font-semibold text-slate-700">{api.records?.toLocaleString()} records</div>
                                                                    <div className="text-sm text-slate-400 mb-2">{api.size_kb} KB</div>
                                                                    <button
                                                                        onClick={() => window.open(`${API_BASE}/api-data/${jobId}/${api.name}?page=1`, '_blank')}
                                                                        title="Open paginated JSON in a new tab"
                                                                        className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-semibold text-emerald-700 bg-emerald-50 rounded-lg hover:bg-emerald-100 transition-colors border border-emerald-200"
                                                                    >
                                                                        <FileJson className="w-4 h-4" />
                                                                        Browse API
                                                                    </button>
                                                                </div>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>
                    </section>
                </main>
            </div>
        </div>
    );
}
