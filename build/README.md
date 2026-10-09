# Building and publishing container images

The image flow has two stages:

1. **Compile** — `cross` builds the target binary inside the per-target image from
   `build/cross/Dockerfile.<arch>`, which statically compiles FFmpeg (+RKMPP/RGA on
   ARM), libyuv, libvpx, x264, x265 and libopus from source. Output:
   `target/<triple>/release/one-kvm`.
2. **Package** — `build/package-docker.sh` stages that binary plus `ttyd` (and
   `gostc`/`easytier-core` for the `full` variant) and runs `docker buildx build`
   with `build/Dockerfile.runtime[-full]`.

The frontend must be built first, because the binary embeds `web/dist`:

```bash
cd web && npm install && npm run build && cd ..
```

## Recommended: build in CI (amd64 runner)

`.github/workflows/docker.yml` runs on GitHub's amd64 runners and publishes:

| Registry | Image | Requirements |
|----------|-------|--------------|
| ghcr.io | `ghcr.io/<owner>/one-kvm`, `ghcr.io/<owner>/one-kvm-full` | none (uses `GITHUB_TOKEN`) |
| Docker Hub | `<user>/one-kvm`, `<user>/one-kvm-full` | `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN` secrets |

Trigger it from **Actions → Docker → Run workflow**, or by pushing a `v*` tag. The
`platforms` input defaults to `linux/arm64`; use `linux/amd64,linux/arm64` for a
multi-arch manifest. Every added platform rebuilds all the codec libraries inside
its own cross image, so the job roughly doubles in length per platform (the
workflow allows up to 6 hours).

## Local build on an amd64 machine

```bash
cargo install cross --version 0.2.5 --locked

./build/build-images.sh arm64                       # or x86_64 / armv7 / all
./build/package-docker.sh --platform linux/arm64 --load --tag local
```

`--load` needs a single platform. To move the image to an arm64 test device, push
it (add `--registry <registry> --push`) and pull it there, or use
`docker save` / `docker load`.

## Build on an aarch64 host

`./build/build-images.sh arm64` **does not work on an arm64 host**, even though the
target matches the host. `cross` derives the build/run platform from the *base
image* (`ghcr.io/cross-rs/<target>:main`, which cross-rs publishes as amd64) rather
than from the host, so it invokes:

```
docker buildx build --platform linux/amd64 --file build/cross/Dockerfile.arm64 ...
```

and then runs the container as amd64 too (`src/docker/local.rs` calls
`specify_platform` for the run step as well). Emulating the whole toolchain is very
slow and QEMU tends to crash on it. `CROSS_BUILD_OPTS="--platform linux/arm64"`
does not help: `--platform` is a buildx *list* flag, so you end up building amd64
and arm64 and the docker exporter fails with *"docker exporter does not currently
support exporting manifest lists"*.

Build the image natively instead, bypassing `cross` — the Dockerfile is
self-contained (it installs its own Rust toolchain and exports the linker,
`PKG_CONFIG` and `RUSTFLAGS` settings it needs):

```bash
docker build --platform linux/arm64 \
  -t one-kvm-cross-arm64:local \
  -f build/cross/Dockerfile.arm64 \
  .

docker run --rm --platform linux/arm64 \
  -v "$PWD":/project -w /project \
  -v one-kvm-cargo-registry:/root/.cargo/registry \
  one-kvm-cross-arm64:local \
  cargo build --release --target aarch64-unknown-linux-gnu

./build/package-docker.sh --platform linux/arm64 --load --tag local
```

The compile step runs as root, so `target/` ends up root-owned; run
`sudo chown -R $(id -u):$(id -g) target` afterwards if that is a problem.

## Running the image

See the Docker section of the top-level README. Note that the built-in
self-update checks `https://update.one-kvm.cn` (override with the
`ONE_KVM_UPDATE_BASE_URL` environment variable); on a fork you probably want to
point that at your own manifest or disable it, otherwise updating would replace
the binary with upstream's build.
