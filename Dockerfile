FROM astrocrpublic.azurecr.io/runtime:3.1-13

# Install Playwright browser binaries. The Astro runtime image already handles OS deps;
# using --with-deps here triggers sudo inside the container and fails during build.
ARG INSTALL_PLAYWRIGHT_CHROMIUM=0
RUN if [ "$INSTALL_PLAYWRIGHT_CHROMIUM" = "1" ]; then \
      python -m playwright install chromium; \
    else \
      echo "Skipping Playwright Chromium install (set INSTALL_PLAYWRIGHT_CHROMIUM=1 to enable legacy fallback worker support)."; \
    fi
