'use client';

import { Suspense, useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { AlertCircle, ExternalLink, FileText, Loader2, Sparkles, Upload } from 'lucide-react';

import { ResumeUploadDialog } from '@/components/dashboard/resume-upload-dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import {
  fetchLaunchContext,
  generateTailoredMaterials,
  getLaunchUploadUrl,
  type LaunchResumeContext,
} from '@/lib/api/integration';
import { fetchResume } from '@/lib/api/resume';

const RESUME_OVERRIDE_ACCEPTED_TYPES = [
  '.md',
  '.txt',
  'text/plain',
  'text/markdown',
  'application/pdf',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/msword',
];

function toExcerpt(content: string | undefined): string | null {
  const flattened = (content || '').replace(/\s+/g, ' ').trim();
  if (!flattened) return null;
  return flattened.length > 280 ? `${flattened.slice(0, 280)}...` : flattened;
}

function LaunchPageContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const token = searchParams.get('token') || '';

  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [launchData, setLaunchData] = useState<Awaited<
    ReturnType<typeof fetchLaunchContext>
  > | null>(null);
  const [selectedResume, setSelectedResume] = useState<LaunchResumeContext | null>(null);
  const [jobDescription, setJobDescription] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function load() {
      if (!token) {
        setError('Missing launch token.');
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        setError(null);
        const data = await fetchLaunchContext(token);
        if (cancelled) return;
        setLaunchData(data);
        setSelectedResume(data.resume);
        setJobDescription(data.job.description || '');
      } catch (err) {
        console.error(err);
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to initialize launch context.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [token]);

  const uploadUrl = useMemo(() => {
    if (!token) return undefined;
    return getLaunchUploadUrl(token);
  }, [token]);

  const handleUploadComplete = async (resumeId: string) => {
    try {
      const data = await fetchResume(resumeId);
      setSelectedResume({
        resume_id: resumeId,
        filename: data.filename ?? 'uploaded-resume.md',
        source_label: 'Uploaded resume',
        is_default: false,
        processing_status: data.raw_resume.processing_status,
        excerpt: toExcerpt(data.raw_resume.content),
      });
    } catch (err) {
      console.error('Failed to refresh uploaded resume:', err);
      setError('Resume uploaded, but the selected resume preview could not be refreshed.');
    }
  };

  const handleGenerate = async () => {
    if (!launchData || !selectedResume || !token) return;

    setGenerating(true);
    setError(null);
    try {
      const result = await generateTailoredMaterials({
        token,
        resume_id: selectedResume.resume_id,
        job_id: launchData.job.job_id,
        job_description: jobDescription,
      });
      router.push(result.builder_path);
    } catch (err) {
      console.error(err);
      setError(err instanceof Error ? err.message : 'Failed to generate tailored materials.');
    } finally {
      setGenerating(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="flex flex-col items-center gap-3">
          <Loader2 className="w-8 h-8 animate-spin text-blue-700" />
          <p className="font-mono text-sm uppercase tracking-wide text-blue-700">
            Loading launch context...
          </p>
        </div>
      </div>
    );
  }

  if (error && !launchData) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background px-4">
        <div className="max-w-xl border border-red-300 bg-red-50 p-6 shadow-sw-default rounded-none">
          <div className="flex items-start gap-3">
            <AlertCircle className="mt-0.5 w-5 h-5 text-red-700" />
            <div>
              <h1 className="font-serif text-xl font-bold text-red-800">Launch failed</h1>
              <p className="mt-2 text-sm text-red-800">{error}</p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!launchData || !selectedResume) {
    return null;
  }

  return (
    <div className="min-h-screen bg-background px-4 py-10">
      <div className="mx-auto flex max-w-5xl flex-col gap-6">
        <div className="border border-black bg-white p-6 shadow-sw-default rounded-none">
          <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
            <div className="space-y-2">
              <p className="font-mono text-xs uppercase tracking-[0.2em] text-steel-grey">
                Resume Matcher launch
              </p>
              <h1 className="font-serif text-3xl font-bold uppercase tracking-tight text-black">
                {launchData.job.title || 'Tailored application materials'}
              </h1>
              <p className="text-sm text-black/70">
                {launchData.job.company || 'Unknown company'} · {launchData.profile.display_name}
              </p>
              <p className="max-w-3xl text-sm text-black/70">
                The job description and your profile resume are preloaded below. Nothing uses the
                LLM until you click <strong>Generate tailored materials</strong>.
              </p>
            </div>
            {launchData.job.job_url ? (
              <a
                href={launchData.job.job_url}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-2 text-sm font-semibold text-blue-700 underline underline-offset-4"
              >
                Open original posting
                <ExternalLink className="w-4 h-4" />
              </a>
            ) : null}
          </div>
        </div>

        {error ? (
          <div className="border border-orange-300 bg-orange-50 px-4 py-3 text-sm text-orange-800 shadow-sw-default rounded-none">
            {error}
          </div>
        ) : null}

        <div className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
          <section className="border border-black bg-white p-6 shadow-sw-default rounded-none">
            <div className="mb-4 flex items-center gap-2">
              <FileText className="w-4 h-4" />
              <h2 className="font-serif text-xl font-bold uppercase tracking-tight">
                Job description
              </h2>
            </div>
            <Textarea
              value={jobDescription}
              onChange={(event) => setJobDescription(event.target.value)}
              className="min-h-[380px] rounded-none border-black font-mono text-sm"
            />
          </section>

          <section className="flex flex-col gap-6">
            <div className="border border-black bg-white p-6 shadow-sw-default rounded-none">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <p className="font-serif text-xl font-bold uppercase tracking-tight">Resume</p>
                  <p className="mt-1 text-sm text-black/70">
                    Default profile resume is loaded, but you can replace it for this launch.
                  </p>
                </div>
                {uploadUrl ? (
                  <ResumeUploadDialog
                    uploadUrl={uploadUrl}
                    acceptedFileTypes={RESUME_OVERRIDE_ACCEPTED_TYPES}
                    onUploadComplete={handleUploadComplete}
                    trigger={
                      <Button variant="outline" size="sm">
                        <Upload className="w-4 h-4 mr-2" />
                        Replace resume
                      </Button>
                    }
                  />
                ) : null}
              </div>

              <div className="border border-dashed border-black/30 bg-paper-tint p-4">
                <div className="flex flex-wrap items-center gap-2 text-xs uppercase tracking-wide text-steel-grey">
                  <span>{selectedResume.source_label}</span>
                  {selectedResume.is_default ? <span>Default</span> : <span>Override</span>}
                  <span>{selectedResume.processing_status}</span>
                </div>
                <h3 className="mt-2 text-sm font-semibold text-black">
                  {selectedResume.filename || 'resume.md'}
                </h3>
                {selectedResume.excerpt ? (
                  <p className="mt-3 text-sm leading-6 text-black/75">{selectedResume.excerpt}</p>
                ) : (
                  <p className="mt-3 text-sm text-black/60">No preview available.</p>
                )}
              </div>
            </div>

            <div className="border border-black bg-white p-6 shadow-sw-default rounded-none">
              <div className="flex items-start gap-3">
                <Sparkles className="mt-0.5 w-5 h-5 text-blue-700" />
                <div className="space-y-2 text-sm text-black/75">
                  <p className="font-semibold text-black">Generate flow</p>
                  <ul className="list-disc space-y-1 pl-5">
                    <li>No resume tailoring happens on page load.</li>
                    <li>
                      The selected resume becomes the active source only for this launch flow.
                    </li>
                    <li>
                      After generation, Resume Matcher opens the normal editor and download UI.
                    </li>
                  </ul>
                </div>
              </div>

              <Button
                className="mt-6 w-full"
                size="lg"
                onClick={handleGenerate}
                disabled={generating || !selectedResume || !jobDescription.trim()}
              >
                {generating ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Generating tailored materials...
                  </>
                ) : (
                  <>
                    <Sparkles className="w-4 h-4 mr-2" />
                    Generate tailored materials
                  </>
                )}
              </Button>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

export default function LaunchPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen flex items-center justify-center bg-background">
          <div className="flex flex-col items-center gap-3">
            <Loader2 className="w-8 h-8 animate-spin text-blue-700" />
            <p className="font-mono text-sm uppercase tracking-wide text-blue-700">
              Loading launch context...
            </p>
          </div>
        </div>
      }
    >
      <LaunchPageContent />
    </Suspense>
  );
}
