FROM python:3.12-slim
WORKDIR /workspace
COPY pyproject.toml ./
RUN python -c "import tomllib,subprocess; p=tomllib.load(open('pyproject.toml','rb'))['project']; subprocess.check_call(['pip','install','--no-cache-dir',*p['dependencies'],*p['optional-dependencies']['dashboard']])"
COPY src ./src
COPY sql ./sql
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit
RUN pip install --no-cache-dir --no-deps -e .
EXPOSE 8501
CMD ["python", "-m", "streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
