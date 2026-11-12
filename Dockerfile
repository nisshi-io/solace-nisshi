# Copyright (c) Solace Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Packages a prebuilt static binary: nothing is compiled here. CI's release
# job (or `just docker-dist` locally) places one per platform at
# dist/<os>/<arch>/nisshi, e.g. dist/linux/arm64/nisshi.

# CA certs are architecture-independent, so this stage runs on the build
# host for every target platform and a multi-platform build needs no QEMU.
FROM --platform=$BUILDPLATFORM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS base
RUN mkdir -p /image/schema /image/data /image/tmp /image/etc && cp -r /etc/ssl /image/etc/

FROM scratch
ARG TARGETPLATFORM
# docker/metadata-action generates title, description, url, source, version, created, revision
# and licenses at publish time, and its labels override these. The labels input of
# .github/actions/publish-image therefore sets url and description again, and those two
# must stay equal to the values here.
LABEL org.opencontainers.image.url="https://nisshi.io" \
      org.opencontainers.image.documentation="https://docs.nisshi.io" \
      org.opencontainers.image.description="A stateless Apache Kafka(R)-compatible broker with pluggable storage." \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=base /image /
# --chmod: the binary arrives via an Actions artifact, which drops the exec bit.
COPY --chmod=755 dist/${TARGETPLATFORM}/nisshi /nisshi
COPY LICENSE /LICENSE
COPY NOTICE /NOTICE
COPY dist/${TARGETPLATFORM}/THIRD-PARTY-LICENSE.html /THIRD-PARTY-LICENSE.html
ENV TMP=/tmp
ENTRYPOINT ["/nisshi"]
CMD ["broker"]
