import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

const WORKSPACE_SESSION_COOKIE_NAME = 'resume_matcher_session';

export function proxy(request: NextRequest) {
  const hasWorkspaceSession = Boolean(request.cookies.get(WORKSPACE_SESSION_COOKIE_NAME));

  if (request.nextUrl.pathname.startsWith('/settings') && hasWorkspaceSession) {
    const redirectUrl = request.nextUrl.clone();
    redirectUrl.pathname = '/dashboard';
    redirectUrl.searchParams.set('settings', 'disabled');
    return NextResponse.redirect(redirectUrl);
  }

  if (hasWorkspaceSession) {
    return NextResponse.next();
  }

  const redirectUrl = request.nextUrl.clone();
  redirectUrl.pathname = '/';
  redirectUrl.searchParams.set('session', 'required');
  return NextResponse.redirect(redirectUrl);
}

export const config = {
  matcher: [
    '/dashboard/:path*',
    '/builder/:path*',
    '/resumes/:path*',
    '/tailor/:path*',
    '/settings/:path*',
    '/print/:path*',
  ],
};
