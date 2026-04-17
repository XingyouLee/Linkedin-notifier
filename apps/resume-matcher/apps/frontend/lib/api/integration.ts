import { API_BASE, apiFetch, apiPost } from './client';

export interface LaunchProfileContext {
  profile_id: number;
  display_name: string;
}

export interface LaunchJobContext {
  canonical_job_id: string;
  job_id: string;
  title?: string | null;
  company?: string | null;
  job_url?: string | null;
  description: string;
}

export interface LaunchResumeContext {
  resume_id: string;
  filename?: string | null;
  source_label: string;
  is_default: boolean;
  processing_status: 'pending' | 'processing' | 'ready' | 'failed';
  excerpt?: string | null;
}

export interface LaunchContextResponse {
  data: {
    profile: LaunchProfileContext;
    job: LaunchJobContext;
    resume: LaunchResumeContext;
  };
}

export interface GenerateMaterialsRequest {
  token: string;
  resume_id: string;
  job_id: string;
  job_description?: string | null;
  prompt_id?: string | null;
}

export interface GenerateMaterialsResponse {
  data: {
    resume_id: string;
    cover_letter_generated: boolean;
    cover_letter_pdf_url?: string | null;
    resume_pdf_url?: string | null;
    builder_path: string;
  };
}

export async function fetchLaunchContext(token: string): Promise<LaunchContextResponse['data']> {
  const res = await apiFetch(
    `/integrations/linkedin-notifier/launch?token=${encodeURIComponent(token)}`
  );
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Failed to initialize launch context (status ${res.status}): ${text}`);
  }
  const payload = (await res.json()) as LaunchContextResponse;
  return payload.data;
}

export async function generateTailoredMaterials(
  payload: GenerateMaterialsRequest
): Promise<GenerateMaterialsResponse['data']> {
  const res = await apiPost('/integrations/linkedin-notifier/generate', payload, 240_000);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Failed to generate tailored materials (status ${res.status}): ${text}`);
  }
  const data = (await res.json()) as GenerateMaterialsResponse;
  return data.data;
}

export function getLaunchUploadUrl(token: string): string {
  return `${API_BASE}/integrations/linkedin-notifier/resumes/upload?token=${encodeURIComponent(token)}`;
}
