import { useState, useCallback } from 'react';
import axios from 'axios';
import CodeMirror from '@uiw/react-codemirror';
import { yaml } from '@codemirror/lang-yaml';
import { Play, Loader2, Database, FileJson, FileText, Clock, AlertCircle, Settings, X, CheckCircle2, Download, Eraser, ShieldCheck, AlertTriangle, Info, ChevronDown, ChevronUp, CalendarDays } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

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

interface ValidationIssue {
    path: string;
    message: string;
}

interface ValidationResult {
    valid: boolean;
    errors: ValidationIssue[];
    warnings: ValidationIssue[];
    summary: {
        entity_count: number;
        column_count: number;
    };
}

export default function App() {
    const [schemaText, setSchemaText] = useState(DEFAULT_SCHEMA);
    const [status, setStatus] = useState<'idle' | 'running' | 'completed' | 'failed'>('idle');
    const [jobId, setJobId] = useState<string | null>(null);
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    // Connection String State
    const [isSettingsOpen, setIsSettingsOpen] = useState(false);
    const [connectionString, setConnectionString] = useState(() => localStorage.getItem('db_connection_string') || '');
    const [testDbStatus, setTestDbStatus] = useState<'idle' | 'testing' | 'success' | 'failed'>('idle');
    const [testDbMessage, setTestDbMessage] = useState('');

    // Validation State
    const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);
    const [isValidating, setIsValidating] = useState(false);
    const [showValidationDetails, setShowValidationDetails] = useState(true);

    // Incremental Data Generation State
    const [targetDateStart, setTargetDateStart] = useState<string>('');
    const [targetDateEnd, setTargetDateEnd] = useState<string>('');
    const [incrementRows, setIncrementRows] = useState(10);

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

    const handleValidate = async (): Promise<ValidationResult | null> => {
        try {
            setIsValidating(true);
            setValidationResult(null);

            const res = await axios.post(`${API_BASE}/validate`, {
                schema: schemaText,
            });
            const vr: ValidationResult = res.data;
            setValidationResult(vr);
            setShowValidationDetails(true);
            return vr;
        } catch (err: any) {
            const errorResult: ValidationResult = {
                valid: false,
                errors: [{ path: '(request)', message: err.response?.data?.detail || err.message }],
                warnings: [],
                summary: { entity_count: 0, column_count: 0 },
            };
            setValidationResult(errorResult);
            setShowValidationDetails(true);
            return errorResult;
        } finally {
            setIsValidating(false);
        }
    };

    const handleMockMe = async () => {
        // Step 1: Validate first
        const vr = await handleValidate();
        if (!vr || !vr.valid) {
            // Don't proceed if validation failed
            return;
        }

        // Step 2: Generate
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

    const getDaysDifference = () => {
        if (!targetDateStart || !targetDateEnd) return -1;
        const start = new Date(targetDateStart);
        start.setHours(0, 0, 0, 0);
        const end = new Date(targetDateEnd);
        end.setHours(0, 0, 0, 0);
        const diffTime = end.getTime() - start.getTime();
        return Math.floor(diffTime / (1000 * 60 * 60 * 24));
    };

    const daysDiff = getDaysDifference();
    const isDateRangeValid = targetDateStart !== '' && targetDateEnd !== '' && daysDiff >= 0 && daysDiff <= 365;
    const isIncrementReady = isDateRangeValid && incrementRows >= 1;

    const handleIncrementalGeneration = async () => {
        if (!jobId || status !== 'completed' || !isIncrementReady) return;
        const baseId = jobId;
        try {
            setStatus('running');
            setError(null);

            const payload = {
                schema: schemaText,
                connection_string: connectionString || undefined,
                base_job_id: baseId,
                target_date_start: targetDateStart,
                target_date_end: targetDateEnd,
                rows_per_day: incrementRows,
            };

            const res = await axios.post(`${API_BASE}/generate-daily`, payload, {
                headers: { 'Content-Type': 'application/json' },
            });
            setJobId(res.data.job_id);
            pollStatus(res.data.job_id);
        } catch (err: any) {
            setError(err.response?.data?.detail || err.message);
            setStatus('failed');
        }
    };

    const handleValidateOnly = async () => {
        await handleValidate();
    };

    // Clear validation when schema text changes
    const handleSchemaChange = (val: string) => {
        setSchemaText(val);
        // Clear stale validation results when schema changes
        if (validationResult) {
            setValidationResult(null);
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
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => setIsSettingsOpen(true)}
                            className="flex items-center text-slate-600 hover:text-slate-900 transition-colors px-4 py-2 rounded-lg hover:bg-slate-100 font-medium"
                        >
                            <Settings className="w-5 h-5 mr-2" />
                            DB Settings
                        </button>
                        <button
                            onClick={handleValidateOnly}
                            disabled={isValidating || status === 'running'}
                            className="group relative inline-flex items-center justify-center px-6 py-3 font-bold text-emerald-700 transition-all duration-200 bg-emerald-50 rounded-xl hover:bg-emerald-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-emerald-500 disabled:opacity-70 disabled:cursor-not-allowed border border-emerald-200 hover:border-emerald-300"
                        >
                            {isValidating ? (
                                <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                            ) : (
                                <ShieldCheck className="w-5 h-5 mr-2 group-hover:scale-110 transition-transform" />
                            )}
                            {isValidating ? 'Checking...' : 'Validate'}
                        </button>
                        <button
                            onClick={handleMockMe}
                            disabled={status === 'running' || isValidating}
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
                                        onClick={() => {
                                            localStorage.removeItem('db_connection_string');
                                            setConnectionString('');
                                        }}
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
                                            onClick={() => {
                                                if (connectionString) {
                                                    localStorage.setItem('db_connection_string', connectionString);
                                                } else {
                                                    localStorage.removeItem('db_connection_string');
                                                }
                                                setIsSettingsOpen(false);
                                            }}
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

                {/* Validation Results Banner */}
                <AnimatePresence>
                    {validationResult && (
                        <motion.div
                            initial={{ opacity: 0, y: -12 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -12 }}
                            transition={{ duration: 0.25 }}
                        >
                            {validationResult.valid ? (
                                /* ── SUCCESS BANNER ── */
                                <div className="rounded-2xl border border-emerald-200 bg-gradient-to-r from-emerald-50 via-green-50 to-teal-50 shadow-sm overflow-hidden">
                                    <div className="px-6 py-4 flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-emerald-100">
                                                <CheckCircle2 className="w-6 h-6 text-emerald-600" />
                                            </div>
                                            <div>
                                                <h3 className="font-bold text-emerald-800 text-base">Schema Valid — Ready to Generate</h3>
                                                <p className="text-sm text-emerald-600 mt-0.5">
                                                    {validationResult.summary.entity_count} {validationResult.summary.entity_count === 1 ? 'entity' : 'entities'} · {validationResult.summary.column_count} {validationResult.summary.column_count === 1 ? 'column' : 'columns'}
                                                    {validationResult.warnings.length > 0 && ` · ${validationResult.warnings.length} ${validationResult.warnings.length === 1 ? 'warning' : 'warnings'}`}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {validationResult.warnings.length > 0 && (
                                                <button
                                                    onClick={() => setShowValidationDetails(!showValidationDetails)}
                                                    className="flex items-center gap-1 text-sm font-medium text-emerald-600 hover:text-emerald-800 transition-colors px-3 py-1.5 rounded-lg hover:bg-emerald-100"
                                                >
                                                    {showValidationDetails ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                                                    {showValidationDetails ? 'Hide' : 'Show'} Warnings
                                                </button>
                                            )}
                                            <button onClick={() => setValidationResult(null)} className="p-1.5 rounded-lg text-emerald-400 hover:text-emerald-600 hover:bg-emerald-100 transition-colors">
                                                <X className="w-4 h-4" />
                                            </button>
                                        </div>
                                    </div>
                                    {/* Warnings List */}
                                    <AnimatePresence>
                                        {showValidationDetails && validationResult.warnings.length > 0 && (
                                            <motion.div
                                                initial={{ height: 0, opacity: 0 }}
                                                animate={{ height: 'auto', opacity: 1 }}
                                                exit={{ height: 0, opacity: 0 }}
                                                transition={{ duration: 0.2 }}
                                                className="overflow-hidden"
                                            >
                                                <div className="px-6 pb-4 space-y-2">
                                                    <div className="border-t border-emerald-200 pt-3" />
                                                    {validationResult.warnings.map((w, i) => (
                                                        <div key={i} className="flex items-start gap-2.5 text-sm pl-1">
                                                            <AlertTriangle className="w-4 h-4 text-amber-500 mt-0.5 shrink-0" />
                                                            <div>
                                                                <code className="text-xs font-mono bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded">{w.path}</code>
                                                                <span className="text-slate-600 ml-2">{w.message}</span>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            ) : (
                                /* ── ERROR BANNER ── */
                                <div className="rounded-2xl border border-red-200 bg-gradient-to-r from-red-50 via-rose-50 to-pink-50 shadow-sm overflow-hidden">
                                    <div className="px-6 py-4 flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-red-100">
                                                <AlertCircle className="w-6 h-6 text-red-600" />
                                            </div>
                                            <div>
                                                <h3 className="font-bold text-red-800 text-base">Schema Has Issues — Please Fix Before Generating</h3>
                                                <p className="text-sm text-red-600 mt-0.5">
                                                    {validationResult.errors.length} {validationResult.errors.length === 1 ? 'error' : 'errors'}
                                                    {validationResult.warnings.length > 0 && ` · ${validationResult.warnings.length} ${validationResult.warnings.length === 1 ? 'warning' : 'warnings'}`}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <button
                                                onClick={() => setShowValidationDetails(!showValidationDetails)}
                                                className="flex items-center gap-1 text-sm font-medium text-red-600 hover:text-red-800 transition-colors px-3 py-1.5 rounded-lg hover:bg-red-100"
                                            >
                                                {showValidationDetails ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                                                {showValidationDetails ? 'Hide' : 'Show'} Details
                                            </button>
                                            <button onClick={() => setValidationResult(null)} className="p-1.5 rounded-lg text-red-400 hover:text-red-600 hover:bg-red-100 transition-colors">
                                                <X className="w-4 h-4" />
                                            </button>
                                        </div>
                                    </div>
                                    <AnimatePresence>
                                        {showValidationDetails && (
                                            <motion.div
                                                initial={{ height: 0, opacity: 0 }}
                                                animate={{ height: 'auto', opacity: 1 }}
                                                exit={{ height: 0, opacity: 0 }}
                                                transition={{ duration: 0.2 }}
                                                className="overflow-hidden"
                                            >
                                                <div className="px-6 pb-4 space-y-2 max-h-72 overflow-y-auto">
                                                    <div className="border-t border-red-200 pt-3" />
                                                    {/* Errors */}
                                                    {validationResult.errors.map((e, i) => (
                                                        <div key={`err-${i}`} className="flex items-start gap-2.5 text-sm pl-1">
                                                            <AlertCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                                                            <div>
                                                                <code className="text-xs font-mono bg-red-100 text-red-700 px-1.5 py-0.5 rounded">{e.path}</code>
                                                                <span className="text-slate-700 ml-2">{e.message}</span>
                                                            </div>
                                                        </div>
                                                    ))}
                                                    {/* Warnings */}
                                                    {validationResult.warnings.map((w, i) => (
                                                        <div key={`warn-${i}`} className="flex items-start gap-2.5 text-sm pl-1">
                                                            <AlertTriangle className="w-4 h-4 text-amber-500 mt-0.5 shrink-0" />
                                                            <div>
                                                                <code className="text-xs font-mono bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded">{w.path}</code>
                                                                <span className="text-slate-600 ml-2">{w.message}</span>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            )}
                        </motion.div>
                    )}
                </AnimatePresence>

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
                                onChange={handleSchemaChange}
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
                                        <p className="text-sm mt-1">Click <strong>"Validate"</strong> to check your schema, or <strong>"Mock Me"</strong> to generate.</p>
                                        <div className="mt-6 flex items-start gap-2 px-6 py-3 bg-slate-50 rounded-xl border border-slate-100 text-sm text-slate-500 max-w-sm">
                                            <Info className="w-4 h-4 mt-0.5 shrink-0 text-violet-400" />
                                            <span>"Mock Me" will automatically validate your schema first. If errors are found, generation will be blocked.</span>
                                        </div>
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

                                        {/* Incremental Data Generation Section */}
                                        <div className="p-5 bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border border-blue-200 shadow-sm">
                                            <div className="mb-3">
                                                <p className="text-sm font-semibold text-blue-700 uppercase tracking-wider mb-1">Date Range Generation</p>
                                                <p className="text-xs text-blue-600">Generate rows chronologically across a specific date window (max span 365 days)</p>
                                            </div>
                                            <div className="flex items-end gap-4 flex-wrap">
                                                <div className="flex flex-col gap-1">
                                                    <label className="text-xs font-medium text-blue-700">Start Date</label>
                                                    <input
                                                        type="date"
                                                        value={targetDateStart}
                                                        onChange={(e) => setTargetDateStart(e.target.value)}
                                                        className={`px-3 py-2 text-sm border rounded-lg focus:ring-2 focus:ring-blue-400 focus:border-blue-400 outline-none bg-white font-semibold transition-colors ${targetDateStart !== '' && !isDateRangeValid && targetDateEnd !== ''
                                                            ? 'border-red-400 bg-red-50'
                                                            : 'border-blue-300'
                                                            }`}
                                                    />
                                                </div>
                                                <div className="flex flex-col gap-1">
                                                    <label className="text-xs font-medium text-blue-700">End Date</label>
                                                    <input
                                                        type="date"
                                                        value={targetDateEnd}
                                                        onChange={(e) => setTargetDateEnd(e.target.value)}
                                                        min={targetDateStart}
                                                        className={`px-3 py-2 text-sm border rounded-lg focus:ring-2 focus:ring-blue-400 focus:border-blue-400 outline-none bg-white font-semibold transition-colors ${targetDateEnd !== '' && !isDateRangeValid && targetDateStart !== ''
                                                            ? 'border-red-400 bg-red-50'
                                                            : 'border-blue-300'
                                                            }`}
                                                    />
                                                    {targetDateStart !== '' && targetDateEnd !== '' && !isDateRangeValid && (
                                                        <span className="text-[10px] text-red-500">Invalid range</span>
                                                    )}
                                                </div>
                                                <div className="flex flex-col gap-1">
                                                    <label className="text-xs font-medium text-blue-700">Rows Per Day</label>
                                                    <input
                                                        type="number"
                                                        min={1}
                                                        value={incrementRows}
                                                        onChange={(e) => setIncrementRows(Math.max(1, parseInt(e.target.value) || 1))}
                                                        className="w-24 px-3 py-2 text-sm border border-blue-300 rounded-lg focus:ring-2 focus:ring-blue-400 focus:border-blue-400 outline-none bg-white text-center font-semibold"
                                                    />
                                                </div>
                                                <button
                                                    onClick={handleIncrementalGeneration}
                                                    disabled={isValidating || !isIncrementReady}
                                                    className="group inline-flex items-center px-5 py-2 text-sm font-bold text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm hover:shadow"
                                                >
                                                    <CalendarDays className="w-4 h-4 mr-1.5 group-hover:scale-110 transition-transform" />
                                                    Generate
                                                </button>
                                            </div>
                                            {isIncrementReady && (
                                                <div className="mt-3 text-xs text-blue-500 bg-blue-100/50 rounded-lg px-3 py-2">
                                                    Will generate <strong>{incrementRows.toLocaleString()}</strong> rows per day for <strong>{daysDiff + 1}</strong> days ({((daysDiff + 1) * incrementRows).toLocaleString()} total rows per entity), from {targetDateStart} to {targetDateEnd}
                                                </div>
                                            )}
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
