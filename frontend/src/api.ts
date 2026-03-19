const API_BASE = import.meta.env.DEV ? '' : ''

export interface PdfVersion {
  id: number
  version_name: string
  edition_year: number | null
  file_size_bytes: number | null
  file_hash?: string
  storage_path?: string
  original_filename?: string
  is_active: boolean
  is_fully_parsed: boolean
  notes?: string | null
  created_at: string | null
}

export interface DiffChange {
  key: Record<string, string | number>
  field: string
  old: number
  new: number
  change: number
  pct_change: number | null
}

export interface DiffSummary {
  old_version_id: number | null
  new_version_id: number
  section?: number
  old_count: number
  new_count: number
  count_delta: number
  old_row_count?: number
  new_row_count?: number
  row_count_delta?: number
  is_first_version: boolean
  sample_changes: DiffChange[]
}

export interface ParseRunInfo {
  parser_name: string
  label: string
  description: string
  section: number | null
  page_type: 'single' | 'range' | 'csv' | null
  default_start_page: number | null
  default_end_page: number | null
  status: 'not_started' | 'pending' | 'running' | 'success' | 'failed'
  records_created: number
  error_message: string | null
  diff_summary: DiffSummary | null
  started_at: string | null
  completed_at: string | null
}

export interface ValidationCheck {
  parser: string
  label: string
  passed: boolean
  reason: string
  expected: number
  actual: number
}

export interface ParserInfo {
  name: string
  label: string
  description: string
  section: number | null
}

export interface DbStats {
  local_multipliers: number
  current_cost_multipliers: number
  story_height_multipliers: { total: number; by_section: Record<string, number> }
  floor_area_perimeter_multipliers: { total: number; by_section: Record<string, number> }
  sprinkler_costs: { total: number; by_section: Record<string, number> }
  hvac_costs: { total: number; by_section: Record<string, number> }
  base_cost_tables: number
  base_cost_rows: number
  elevator_types: number
  elevator_costs: number
}

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, options)
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`${res.status}: ${body}`)
  }
  return res.json()
}

export async function fetchVersions(): Promise<PdfVersion[]> {
  const data = await apiFetch<{ versions: PdfVersion[] }>('/pdf-versions')
  return data.versions
}

export async function fetchVersion(id: number): Promise<PdfVersion> {
  return apiFetch<PdfVersion>(`/pdf-versions/${id}`)
}

export async function uploadVersion(
  file: File,
  versionName: string,
  editionYear: number | null,
  notes: string
): Promise<{ id: number; version_name: string; already_existed: boolean }> {
  const formData = new FormData()
  formData.append('pdf_file', file)
  if (versionName) formData.append('version_name', versionName)
  if (editionYear) formData.append('edition_year', String(editionYear))
  if (notes) formData.append('notes', notes)

  return apiFetch('/pdf-versions/upload', { method: 'POST', body: formData })
}

export async function activateVersion(id: number, force = false): Promise<void> {
  const url = force ? `/pdf-versions/${id}/activate?force=true` : `/pdf-versions/${id}/activate`
  await apiFetch(url, { method: 'PATCH' })
}

export async function deleteVersion(id: number): Promise<void> {
  await apiFetch(`/pdf-versions/${id}`, { method: 'DELETE' })
}

export async function fetchParseRuns(versionId: number): Promise<ParseRunInfo[]> {
  const data = await apiFetch<{ parse_runs: ParseRunInfo[] }>(`/pdf-versions/${versionId}/parse-runs`)
  return data.parse_runs
}

export interface RunParserPageOpts {
  startPage?: number
  endPage?: number
}

export async function runParser(versionId: number, parserName: string, pageOpts?: RunParserPageOpts): Promise<unknown> {
  // Map parser names to their base API endpoints
  const endpointMap: Record<string, string> = {
    local_multipliers: `/parse-version/${versionId}/local-multipliers`,
    current_cost: `/parse-version/${versionId}/current-cost`,
    story_height_s11: `/parse-version/${versionId}/story-height?section=11`,
    story_height_s13: `/parse-version/${versionId}/story-height?section=13`,
    story_height_s14: `/parse-version/${versionId}/story-height?section=14`,
    story_height_s15: `/parse-version/${versionId}/story-height?section=15`,
    floor_area_perimeter_s11: `/parse-version/${versionId}/floor-area-perimeter?section=11`,
    floor_area_perimeter_s13: `/parse-version/${versionId}/floor-area-perimeter?section=13`,
    floor_area_perimeter_s14: `/parse-version/${versionId}/floor-area-perimeter?section=14`,
    floor_area_perimeter_s15: `/parse-version/${versionId}/floor-area-perimeter?section=15`,
  }

  let endpoint = endpointMap[parserName]
  if (!endpoint) {
    throw new Error(`No endpoint mapping for parser: ${parserName}`)
  }

  // Append page override query params based on parser type
  if (pageOpts) {
    const sep = endpoint.includes('?') ? '&' : '?'
    const params: string[] = []

    if (parserName === 'local_multipliers') {
      if (pageOpts.startPage) params.push(`start_page=${pageOpts.startPage}`)
      if (pageOpts.endPage) params.push(`end_page=${pageOpts.endPage}`)
    } else if (parserName === 'current_cost') {
      if (pageOpts.startPage) params.push(`page=${pageOpts.startPage}`)
    } else if (parserName.startsWith('story_height_')) {
      if (pageOpts.startPage) params.push(`start_page=${pageOpts.startPage}`)
    } else if (parserName.startsWith('floor_area_perimeter_')) {
      // Build comma-separated pages string
      const pageList: number[] = []
      if (pageOpts.startPage) pageList.push(pageOpts.startPage)
      if (pageOpts.endPage) pageList.push(pageOpts.endPage)
      if (pageList.length > 0) params.push(`pages=${pageList.join(',')}`)
    }

    if (params.length > 0) {
      endpoint += sep + params.join('&')
    }
  }

  return apiFetch(endpoint, { method: 'POST' })
}

export async function runAllParsers(versionId: number): Promise<unknown> {
  return apiFetch(`/parse-version/${versionId}/all`, { method: 'POST' })
}

export async function validateVersion(versionId: number): Promise<{ all_passed: boolean; checks: ValidationCheck[] }> {
  return apiFetch(`/pdf-versions/${versionId}/validate`, { method: 'POST' })
}

export async function fetchStats(): Promise<DbStats> {
  return apiFetch('/stats')
}

export async function fetchParsers(): Promise<ParserInfo[]> {
  const data = await apiFetch<{ parsers: ParserInfo[] }>('/parsers')
  return data.parsers
}
