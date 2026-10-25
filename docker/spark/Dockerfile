FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY pyproject.toml ./
RUN python -c "import tomllib,subprocess; p=tomllib.load(open('pyproject.toml','rb'))['project']; subprocess.check_call(['pip','install','--no-cache-dir',*p['dependencies'],*p['optional-dependencies']['spark'],*p['optional-dependencies']['dev']])"
ENV PYTHONPATH=/workspace/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 SPARK_LOCAL_IP=127.0.0.1
CMD ["python", "-m", "quaketrack.spark_demo", "--input", "/inputs/silver-snapshot.json", "--output-dir", "/outputs"]
