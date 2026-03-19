import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Alert,
  CircularProgress,
  Tooltip,
  Stack,
} from '@mui/material'
import UploadFileIcon from '@mui/icons-material/UploadFile'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import OpenInNewIcon from '@mui/icons-material/OpenInNew'
import StorageIcon from '@mui/icons-material/Storage'
import {
  PdfVersion,
  DbStats,
  fetchVersions,
  uploadVersion,
  activateVersion,
  deleteVersion,
  fetchStats,
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

export default function VersionListPage() {
  const navigate = useNavigate()
  const [versions, setVersions] = useState<PdfVersion[]>([])
  const [stats, setStats] = useState<DbStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [uploadFile, setUploadFile] = useState<File | null>(null)
  const [uploadName, setUploadName] = useState('')
  const [uploadYear, setUploadYear] = useState('')
  const [uploadNotes, setUploadNotes] = useState('')
  const [uploading, setUploading] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const loadData = useCallback(async () => {
    try {
      setLoading(true)
      const [v, s] = await Promise.all([fetchVersions(), fetchStats()])
      setVersions(v)
      setStats(s)
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadData()
  }, [loadData])

  const handleUpload = async () => {
    if (!uploadFile) return
    setUploading(true)
    setActionError(null)
    try {
      const yearNum = uploadYear ? parseInt(uploadYear, 10) : null
      await uploadVersion(uploadFile, uploadName, yearNum, uploadNotes)
      setUploadOpen(false)
      setUploadFile(null)
      setUploadName('')
      setUploadYear('')
      setUploadNotes('')
      await loadData()
    } catch (e) {
      setActionError(String(e))
    } finally {
      setUploading(false)
    }
  }

  const handleActivate = async (id: number) => {
    setActionError(null)
    try {
      await activateVersion(id)
      await loadData()
    } catch (e) {
      setActionError(String(e))
    }
  }

  const handleDelete = async (id: number, name: string) => {
    if (!window.confirm(`Delete version "${name}"? This cannot be undone.`)) return
    setActionError(null)
    try {
      await deleteVersion(id)
      await loadData()
    } catch (e) {
      setActionError(String(e))
    }
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            PDF Versions
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Upload, manage, and parse MVS PDF editions
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<UploadFileIcon />}
          onClick={() => setUploadOpen(true)}
          size="large"
        >
          Upload New Version
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {actionError && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setActionError(null)}>{actionError}</Alert>}

      {/* DB Stats Summary */}
      {stats && (
        <Paper sx={{ p: 2.5, mb: 3 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
            <StorageIcon fontSize="small" color="primary" />
            <Typography variant="subtitle1">Database Summary</Typography>
          </Box>
          <Box sx={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
            {[
              { label: 'Local Multipliers', value: stats.local_multipliers },
              { label: 'Current Cost', value: stats.current_cost_multipliers },
              { label: 'Story Height', value: stats.story_height_multipliers.total },
              { label: 'Floor Area/Perim', value: stats.floor_area_perimeter_multipliers.total },
              { label: 'Sprinklers', value: stats.sprinkler_costs.total },
              { label: 'HVAC', value: stats.hvac_costs.total },
              { label: 'Base Cost Tables', value: stats.base_cost_tables },
              { label: 'Elevator Types', value: stats.elevator_types },
            ].map((s) => (
              <Box key={s.label} sx={{ textAlign: 'center', minWidth: 100 }}>
                <Typography variant="h6" color="primary.main">{s.value.toLocaleString()}</Typography>
                <Typography variant="caption" color="text.secondary">{s.label}</Typography>
              </Box>
            ))}
          </Box>
        </Paper>
      )}

      {/* Version Table */}
      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
          <CircularProgress />
        </Box>
      ) : versions.length === 0 ? (
        <Paper sx={{ p: 6, textAlign: 'center' }}>
          <UploadFileIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
          <Typography variant="h6" gutterBottom>No PDF versions yet</Typography>
          <Typography color="text.secondary" sx={{ mb: 3 }}>
            Upload an MVS PDF to get started with parsing
          </Typography>
          <Button variant="contained" startIcon={<UploadFileIcon />} onClick={() => setUploadOpen(true)}>
            Upload First Version
          </Button>
        </Paper>
      ) : (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow sx={{ bgcolor: 'grey.50' }}>
                <TableCell width={50}></TableCell>
                <TableCell>Version Name</TableCell>
                <TableCell>Edition Year</TableCell>
                <TableCell>File Size</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Uploaded</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {versions.map((v) => (
                <TableRow
                  key={v.id}
                  hover
                  sx={{
                    cursor: 'pointer',
                    bgcolor: v.is_active ? 'primary.50' : undefined,
                    '&:hover': { bgcolor: v.is_active ? '#e8ecf8' : undefined },
                  }}
                  onClick={() => navigate(`/versions/${v.id}`)}
                >
                  <TableCell>
                    {v.is_active ? (
                      <Tooltip title="Active version">
                        <StarIcon sx={{ color: 'primary.main' }} />
                      </Tooltip>
                    ) : (
                      <StarBorderIcon sx={{ color: 'grey.300' }} />
                    )}
                  </TableCell>
                  <TableCell>
                    <Stack direction="row" alignItems="center" spacing={1}>
                      <Typography variant="body2" fontWeight={600}>{v.version_name}</Typography>
                      {v.is_active && (
                        <Chip label="ACTIVE" size="small" color="primary" sx={{ height: 20, fontSize: 11 }} />
                      )}
                    </Stack>
                  </TableCell>
                  <TableCell>{v.edition_year || '--'}</TableCell>
                  <TableCell>{formatBytes(v.file_size_bytes)}</TableCell>
                  <TableCell>
                    {v.is_fully_parsed ? (
                      <Chip icon={<CheckCircleIcon />} label="Fully Parsed" size="small" color="success" variant="outlined" />
                    ) : (
                      <Chip icon={<ErrorIcon />} label="Incomplete" size="small" color="warning" variant="outlined" />
                    )}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">{formatDate(v.created_at)}</Typography>
                  </TableCell>
                  <TableCell align="right" onClick={(e) => e.stopPropagation()}>
                    <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                      <Tooltip title="View & Parse">
                        <IconButton size="small" onClick={() => navigate(`/versions/${v.id}`)}>
                          <OpenInNewIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      {!v.is_active && (
                        <Tooltip title="Set as Active">
                          <IconButton size="small" color="primary" onClick={() => handleActivate(v.id)}>
                            <StarBorderIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      )}
                      {!v.is_active && (
                        <Tooltip title="Delete">
                          <IconButton size="small" color="error" onClick={() => handleDelete(v.id, v.version_name)}>
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      )}
                    </Stack>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      {/* Upload Dialog */}
      <Dialog open={uploadOpen} onClose={() => setUploadOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Upload New MVS PDF</DialogTitle>
        <DialogContent>
          <Stack spacing={2.5} sx={{ mt: 1 }}>
            {actionError && <Alert severity="error">{actionError}</Alert>}

            <Box
              sx={{
                border: '2px dashed',
                borderColor: uploadFile ? 'primary.main' : 'grey.300',
                borderRadius: 2,
                p: 3,
                textAlign: 'center',
                bgcolor: uploadFile ? 'primary.50' : 'grey.50',
                cursor: 'pointer',
                transition: 'all 0.2s',
                '&:hover': { borderColor: 'primary.main', bgcolor: 'primary.50' },
              }}
              onClick={() => document.getElementById('pdf-upload-input')?.click()}
            >
              <input
                id="pdf-upload-input"
                type="file"
                accept=".pdf"
                hidden
                onChange={(e) => {
                  const f = e.target.files?.[0]
                  if (f) {
                    setUploadFile(f)
                    if (!uploadName) setUploadName(f.name.replace('.pdf', ''))
                  }
                }}
              />
              <UploadFileIcon sx={{ fontSize: 40, color: uploadFile ? 'primary.main' : 'grey.400', mb: 1 }} />
              {uploadFile ? (
                <Typography variant="body2" fontWeight={600} color="primary.main">
                  {uploadFile.name} ({formatBytes(uploadFile.size)})
                </Typography>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click to select a PDF file
                </Typography>
              )}
            </Box>

            <TextField
              label="Version Name"
              value={uploadName}
              onChange={(e) => setUploadName(e.target.value)}
              fullWidth
              size="small"
              placeholder="e.g. MVS Q1 2026"
            />
            <TextField
              label="Edition Year"
              value={uploadYear}
              onChange={(e) => setUploadYear(e.target.value)}
              fullWidth
              size="small"
              type="number"
              placeholder="e.g. 2026"
            />
            <TextField
              label="Notes (optional)"
              value={uploadNotes}
              onChange={(e) => setUploadNotes(e.target.value)}
              fullWidth
              size="small"
              multiline
              rows={2}
              placeholder="Any notes about this version..."
            />
          </Stack>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setUploadOpen(false)} disabled={uploading}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleUpload}
            disabled={!uploadFile || uploading}
            startIcon={uploading ? <CircularProgress size={16} /> : <UploadFileIcon />}
          >
            {uploading ? 'Uploading...' : 'Upload'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
