import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Box,
  Typography,
  Paper,
  Button,
  Chip,
  Alert,
  CircularProgress,
  Divider,
  LinearProgress,
  Tooltip,
  Stack,
  Collapse,
  IconButton,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PlaylistPlayIcon from '@mui/icons-material/PlaylistPlay'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import CancelIcon from '@mui/icons-material/Cancel'
import HourglassEmptyIcon from '@mui/icons-material/HourglassEmpty'
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked'
import VerifiedIcon from '@mui/icons-material/Verified'
import RefreshIcon from '@mui/icons-material/Refresh'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import ExpandLessIcon from '@mui/icons-material/ExpandLess'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import {
  PdfVersion,
  ParseRunInfo,
  ValidationCheck,
  fetchVersion,
  fetchParseRuns,
  runParser,
  runAllParsers,
  validateVersion,
  activateVersion,
} from '../api'

function formatBytes(bytes: number | null): string {
  if (!bytes) return '--'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function formatDate(iso: string | null): string {
  if (!iso) return '--'
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case 'success':
      return <CheckCircleIcon sx={{ color: 'success.main', fontSize: 22 }} />
    case 'failed':
      return <CancelIcon sx={{ color: 'error.main', fontSize: 22 }} />
    case 'running':
      return <CircularProgress size={18} thickness={5} />
    case 'pending':
      return <HourglassEmptyIcon sx={{ color: 'warning.main', fontSize: 22 }} />
    default:
      return <RadioButtonUncheckedIcon sx={{ color: 'grey.400', fontSize: 22 }} />
  }
}

function statusLabel(status: string): string {
  switch (status) {
    case 'success': return 'Completed'
    case 'failed': return 'Failed'
    case 'running': return 'Running...'
    case 'pending': return 'Pending'
    default: return 'Not Started'
  }
}

function statusColor(status: string): 'success' | 'error' | 'warning' | 'info' | 'default' {
  switch (status) {
    case 'success': return 'success'
    case 'failed': return 'error'
    case 'running': return 'info'
    case 'pending': return 'warning'
    default: return 'default'
  }
}

// Group parsers by category for visual organization
function groupParsers(runs: ParseRunInfo[]): { label: string; parsers: ParseRunInfo[] }[] {
  const groups: { label: string; filter: (p: ParseRunInfo) => boolean }[] = [
    { label: 'Global Multipliers', filter: (p) => ['local_multipliers', 'current_cost'].includes(p.parser_name) },
    { label: 'Story Height Multipliers', filter: (p) => p.parser_name.startsWith('story_height_') },
    { label: 'Floor Area / Perimeter Multipliers', filter: (p) => p.parser_name.startsWith('floor_area_perimeter_') },
    { label: 'Base Cost Tables', filter: (p) => p.parser_name.startsWith('base_cost_tables_') },
    { label: 'Refinements & Equipment', filter: (p) => ['sprinklers', 'hvac', 'elevators'].includes(p.parser_name) },
  ]
  return groups.map((g) => ({
    label: g.label,
    parsers: runs.filter(g.filter),
  })).filter((g) => g.parsers.length > 0)
}

export default function VersionDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const versionId = Number(id)

  const [version, setVersion] = useState<PdfVersion | null>(null)
  const [parseRuns, setParseRuns] = useState<ParseRunInfo[]>([])
  const [validation, setValidation] = useState<{ all_passed: boolean; checks: ValidationCheck[] } | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [runningParsers, setRunningParsers] = useState<Set<string>>(new Set())
  const [runAllActive, setRunAllActive] = useState(false)
  const [validating, setValidating] = useState(false)
  const [showValidation, setShowValidation] = useState(false)

  const loadData = useCallback(async () => {
    try {
      setLoading(true)
      const [v, runs] = await Promise.all([fetchVersion(versionId), fetchParseRuns(versionId)])
      setVersion(v)
      setParseRuns(runs)
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [versionId])

  useEffect(() => {
    loadData()
  }, [loadData])

  // Poll for updates while any parser is running
  useEffect(() => {
    const hasRunning = parseRuns.some((r) => r.status === 'running') || runAllActive
    if (!hasRunning) return

    const interval = setInterval(async () => {
      try {
        const runs = await fetchParseRuns(versionId)
        setParseRuns(runs)
        const stillRunning = runs.some((r) => r.status === 'running')
        if (!stillRunning) {
          setRunAllActive(false)
          setRunningParsers(new Set())
          // Refresh version to get updated is_fully_parsed
          const v = await fetchVersion(versionId)
          setVersion(v)
        }
      } catch (_) {
        // ignore polling errors
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [parseRuns, runAllActive, versionId])

  const handleRunParser = async (parserName: string) => {
    setActionError(null)
    setRunningParsers((prev) => new Set(prev).add(parserName))
    try {
      await runParser(versionId, parserName)
      await loadData()
    } catch (e) {
      setActionError(String(e))
    } finally {
      setRunningParsers((prev) => {
        const next = new Set(prev)
        next.delete(parserName)
        return next
      })
    }
  }

  const handleRunAll = async () => {
    setActionError(null)
    setRunAllActive(true)
    try {
      await runAllParsers(versionId)
      await loadData()
    } catch (e) {
      setActionError(String(e))
    } finally {
      setRunAllActive(false)
    }
  }

  const handleValidate = async () => {
    setActionError(null)
    setValidating(true)
    setShowValidation(true)
    try {
      const result = await validateVersion(versionId)
      setValidation(result)
    } catch (e) {
      setActionError(String(e))
    } finally {
      setValidating(false)
    }
  }

  const handleActivate = async () => {
    if (!window.confirm('Set this version as the active MVS data source?')) return
    setActionError(null)
    try {
      await activateVersion(versionId)
      await loadData()
    } catch (e) {
      setActionError(String(e))
    }
  }

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
        <CircularProgress />
      </Box>
    )
  }

  if (error || !version) {
    return (
      <Box>
        <Button startIcon={<ArrowBackIcon />} onClick={() => navigate('/')} sx={{ mb: 2 }}>Back</Button>
        <Alert severity="error">{error || 'Version not found'}</Alert>
      </Box>
    )
  }

  const completedCount = parseRuns.filter((r) => r.status === 'success').length
  const totalCount = parseRuns.length
  const progressPercent = totalCount > 0 ? (completedCount / totalCount) * 100 : 0
  const groups = groupParsers(parseRuns)

  return (
    <Box>
      {/* Back button & header */}
      <Button startIcon={<ArrowBackIcon />} onClick={() => navigate('/')} sx={{ mb: 2 }}>
        All Versions
      </Button>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <Box>
            <Stack direction="row" alignItems="center" spacing={1.5} sx={{ mb: 1 }}>
              <Typography variant="h4">{version.version_name}</Typography>
              {version.is_active && <Chip label="ACTIVE" color="primary" size="small" />}
              {version.is_fully_parsed ? (
                <Chip icon={<CheckCircleIcon />} label="Fully Parsed" size="small" color="success" variant="outlined" />
              ) : (
                <Chip icon={<WarningAmberIcon />} label="Incomplete" size="small" color="warning" variant="outlined" />
              )}
            </Stack>
            <Stack direction="row" spacing={3}>
              {version.edition_year && (
                <Typography variant="body2" color="text.secondary">
                  <strong>Edition:</strong> {version.edition_year}
                </Typography>
              )}
              <Typography variant="body2" color="text.secondary">
                <strong>Size:</strong> {formatBytes(version.file_size_bytes)}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                <strong>Uploaded:</strong> {formatDate(version.created_at)}
              </Typography>
              {version.file_hash && (
                <Typography variant="body2" color="text.secondary">
                  <strong>SHA-256:</strong> {version.file_hash.substring(0, 12)}...
                </Typography>
              )}
            </Stack>
            {version.notes && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1, fontStyle: 'italic' }}>
                {version.notes}
              </Typography>
            )}
          </Box>
          <Stack direction="row" spacing={1}>
            {!version.is_active && (
              <Button
                variant="outlined"
                startIcon={<StarBorderIcon />}
                onClick={handleActivate}
              >
                Set Active
              </Button>
            )}
            {version.is_active && (
              <Chip icon={<StarIcon />} label="Active Version" color="primary" />
            )}
          </Stack>
        </Box>
      </Paper>

      {actionError && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setActionError(null)}>{actionError}</Alert>}

      {/* Progress Overview */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
          <Box>
            <Typography variant="h6" sx={{ mb: 0.5 }}>Parsing Progress</Typography>
            <Typography variant="body2" color="text.secondary">
              {completedCount} of {totalCount} parsers completed
            </Typography>
          </Box>
          <Stack direction="row" spacing={1}>
            <Tooltip title="Refresh status">
              <IconButton onClick={loadData} size="small">
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            <Button
              variant="contained"
              startIcon={runAllActive ? <CircularProgress size={16} sx={{ color: '#fff' }} /> : <PlaylistPlayIcon />}
              onClick={handleRunAll}
              disabled={runAllActive}
            >
              {runAllActive ? 'Running All...' : 'Run All Parsers'}
            </Button>
          </Stack>
        </Box>

        <Box sx={{ mb: 2 }}>
          <LinearProgress
            variant="determinate"
            value={progressPercent}
            sx={{
              height: 10,
              borderRadius: 5,
              bgcolor: 'grey.100',
              '& .MuiLinearProgress-bar': {
                borderRadius: 5,
                bgcolor: completedCount === totalCount ? 'success.main' : 'primary.main',
              },
            }}
          />
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 0.5 }}>
            <Typography variant="caption" color="text.secondary">{Math.round(progressPercent)}%</Typography>
            <Stack direction="row" spacing={2}>
              {[
                { label: 'Completed', count: parseRuns.filter((r) => r.status === 'success').length, color: 'success.main' },
                { label: 'Failed', count: parseRuns.filter((r) => r.status === 'failed').length, color: 'error.main' },
                { label: 'Running', count: parseRuns.filter((r) => r.status === 'running').length, color: 'info.main' },
                { label: 'Not Started', count: parseRuns.filter((r) => r.status === 'not_started').length, color: 'grey.400' },
              ].filter((s) => s.count > 0).map((s) => (
                <Stack key={s.label} direction="row" alignItems="center" spacing={0.5}>
                  <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: s.color }} />
                  <Typography variant="caption" color="text.secondary">{s.count} {s.label}</Typography>
                </Stack>
              ))}
            </Stack>
          </Box>
        </Box>
      </Paper>

      {/* Parser Groups */}
      {groups.map((group) => (
        <Paper key={group.label} sx={{ mb: 2 }}>
          <Box sx={{ px: 3, py: 1.5, bgcolor: 'grey.50', borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="subtitle1">{group.label}</Typography>
          </Box>
          {group.parsers.map((run, idx) => (
            <Box key={run.parser_name}>
              {idx > 0 && <Divider />}
              <Box sx={{ px: 3, py: 2, display: 'flex', alignItems: 'center', gap: 2 }}>
                <StatusIcon status={run.status} />
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Stack direction="row" alignItems="center" spacing={1}>
                    <Typography variant="body2" fontWeight={600}>{run.label}</Typography>
                    <Chip
                      label={statusLabel(run.status)}
                      size="small"
                      color={statusColor(run.status)}
                      variant={run.status === 'not_started' ? 'outlined' : 'filled'}
                      sx={{ height: 20, fontSize: 11 }}
                    />
                    {run.section !== null && (
                      <Chip label={`S${run.section}`} size="small" variant="outlined" sx={{ height: 20, fontSize: 11 }} />
                    )}
                  </Stack>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.25 }}>
                    {run.description}
                  </Typography>
                  {run.status === 'success' && (
                    <Typography variant="caption" color="success.main" fontWeight={600}>
                      {run.records_created.toLocaleString()} records -- completed {formatDate(run.completed_at)}
                    </Typography>
                  )}
                  {run.status === 'failed' && run.error_message && (
                    <Typography variant="caption" color="error.main" sx={{ display: 'block', mt: 0.5, wordBreak: 'break-all' }}>
                      Error: {run.error_message}
                    </Typography>
                  )}
                </Box>
                <Button
                  variant="outlined"
                  size="small"
                  startIcon={
                    runningParsers.has(run.parser_name) || run.status === 'running'
                      ? <CircularProgress size={14} />
                      : <PlayArrowIcon />
                  }
                  disabled={runningParsers.has(run.parser_name) || run.status === 'running' || runAllActive}
                  onClick={() => handleRunParser(run.parser_name)}
                  sx={{ minWidth: 80, flexShrink: 0 }}
                >
                  {run.status === 'success' ? 'Re-run' : 'Run'}
                </Button>
              </Box>
            </Box>
          ))}
        </Paper>
      ))}

      {/* Validation Section */}
      <Paper sx={{ mb: 3 }}>
        <Box
          sx={{
            px: 3,
            py: 2,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            cursor: 'pointer',
          }}
          onClick={() => setShowValidation(!showValidation)}
        >
          <Stack direction="row" alignItems="center" spacing={1.5}>
            <VerifiedIcon color={validation?.all_passed ? 'success' : 'action'} />
            <Box>
              <Typography variant="h6">Validation Checks</Typography>
              <Typography variant="caption" color="text.secondary">
                Verify parsed data meets expected record count minimums
              </Typography>
            </Box>
            {validation && (
              <Chip
                label={validation.all_passed ? 'ALL PASSED' : `${validation.checks.filter((c) => !c.passed).length} FAILED`}
                size="small"
                color={validation.all_passed ? 'success' : 'error'}
                sx={{ ml: 1 }}
              />
            )}
          </Stack>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Button
              variant="outlined"
              size="small"
              startIcon={validating ? <CircularProgress size={14} /> : <VerifiedIcon />}
              disabled={validating}
              onClick={(e) => { e.stopPropagation(); handleValidate() }}
            >
              {validating ? 'Validating...' : 'Run Validation'}
            </Button>
            <IconButton size="small">
              {showValidation ? <ExpandLessIcon /> : <ExpandMoreIcon />}
            </IconButton>
          </Stack>
        </Box>

        <Collapse in={showValidation}>
          <Divider />
          {!validation ? (
            <Box sx={{ p: 3, textAlign: 'center' }}>
              <Typography variant="body2" color="text.secondary">
                Click "Run Validation" to check parsed data integrity
              </Typography>
            </Box>
          ) : (
            <Box>
              {validation.all_passed && (
                <Alert severity="success" sx={{ mx: 2, mt: 2, mb: 1 }}>
                  All {validation.checks.length} validation checks passed. This version is ready to be activated.
                </Alert>
              )}
              {!validation.all_passed && (
                <Alert severity="warning" sx={{ mx: 2, mt: 2, mb: 1 }}>
                  {validation.checks.filter((c) => !c.passed).length} of {validation.checks.length} checks failed.
                  Run the failed parsers and re-validate.
                </Alert>
              )}
              {validation.checks.map((check) => (
                <Box key={check.parser}>
                  <Divider />
                  <Box sx={{ px: 3, py: 1.5, display: 'flex', alignItems: 'center', gap: 2 }}>
                    {check.passed ? (
                      <CheckCircleIcon sx={{ color: 'success.main', fontSize: 20 }} />
                    ) : (
                      <CancelIcon sx={{ color: 'error.main', fontSize: 20 }} />
                    )}
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="body2" fontWeight={600}>{check.label}</Typography>
                      <Typography variant="caption" color={check.passed ? 'text.secondary' : 'error.main'}>
                        {check.reason}
                      </Typography>
                    </Box>
                    <Stack direction="row" spacing={2} alignItems="center">
                      <Box sx={{ textAlign: 'center', minWidth: 60 }}>
                        <Typography variant="body2" fontWeight={700} color={check.passed ? 'success.main' : 'error.main'}>
                          {check.actual.toLocaleString()}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">actual</Typography>
                      </Box>
                      <Box sx={{ textAlign: 'center', minWidth: 60 }}>
                        <Typography variant="body2" color="text.secondary">{check.expected.toLocaleString()}</Typography>
                        <Typography variant="caption" color="text.secondary">expected min</Typography>
                      </Box>
                    </Stack>
                  </Box>
                </Box>
              ))}
            </Box>
          )}
        </Collapse>
      </Paper>
    </Box>
  )
}
