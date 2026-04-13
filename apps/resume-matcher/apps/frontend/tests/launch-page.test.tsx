import React from 'react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import LaunchPage from '@/app/(default)/launch/page';

const pushMock = vi.fn();
const fetchLaunchContextMock = vi.fn();
const generateTailoredMaterialsMock = vi.fn();
const getLaunchUploadUrlMock = vi.fn<(token: string) => string>(() => '/upload');
const fetchResumeMock = vi.fn();

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: pushMock }),
  useSearchParams: () => new URLSearchParams('token=launch-token'),
}));

vi.mock('@/lib/api/integration', () => ({
  fetchLaunchContext: (token: string) => fetchLaunchContextMock(token),
  generateTailoredMaterials: (payload: unknown) => generateTailoredMaterialsMock(payload),
  getLaunchUploadUrl: (token: string) => getLaunchUploadUrlMock(token),
}));

vi.mock('@/lib/api/resume', () => ({
  fetchResume: (resumeId: string) => fetchResumeMock(resumeId),
}));

vi.mock('@/components/dashboard/resume-upload-dialog', () => ({
  ResumeUploadDialog: ({ trigger }: { trigger: React.ReactNode }) => <div>{trigger}</div>,
}));

describe('LaunchPage', () => {
  beforeEach(() => {
    pushMock.mockReset();
    fetchLaunchContextMock.mockReset();
    generateTailoredMaterialsMock.mockReset();
    fetchResumeMock.mockReset();
    fetchLaunchContextMock.mockResolvedValue({
      profile: {
        profile_id: 1,
        display_name: 'Levi',
        canonical_resume_path: '/resume.md',
      },
      job: {
        canonical_job_id: 'job-1',
        job_id: 'workspace-job-1',
        title: 'Data Engineer',
        company: 'Example Co',
        job_url: 'https://example.com/job-1',
        description: 'Build reliable pipelines.',
      },
      resume: {
        resume_id: 'resume-1',
        filename: 'resume.md',
        source_label: 'Profile resume',
        is_default: true,
        processing_status: 'ready',
        excerpt: 'Example resume excerpt',
      },
    });
    generateTailoredMaterialsMock.mockResolvedValue({
      resume_id: 'tailored-1',
      cover_letter_generated: true,
      builder_path: '/builder?id=tailored-1',
    });
  });

  it('does not generate tailored materials on page load', async () => {
    render(<LaunchPage />);

    await screen.findByText('Resume Matcher launch');

    expect(fetchLaunchContextMock).toHaveBeenCalledWith('launch-token');
    expect(generateTailoredMaterialsMock).not.toHaveBeenCalled();
    expect(screen.getByText(/Nothing uses the LLM until you click/i)).toBeInTheDocument();
  });

  it('only generates after the user clicks Generate tailored materials', async () => {
    render(<LaunchPage />);

    const button = await screen.findByRole('button', {
      name: /Generate tailored materials/i,
    });

    expect(generateTailoredMaterialsMock).not.toHaveBeenCalled();

    fireEvent.click(button);

    await waitFor(() => {
      expect(generateTailoredMaterialsMock).toHaveBeenCalledWith({
        token: 'launch-token',
        resume_id: 'resume-1',
        job_id: 'workspace-job-1',
        job_description: 'Build reliable pipelines.',
      });
    });
    expect(pushMock).toHaveBeenCalledWith('/builder?id=tailored-1');
  });
});
