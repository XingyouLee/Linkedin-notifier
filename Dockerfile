FROM astrocrpublic.azurecr.io/runtime:3.1-13

# Install Playwright browser binaries. The Astro runtime image already handles OS deps;
# using --with-deps here triggers sudo inside the container and fails during build.
RUN python -m playwright install chromium
