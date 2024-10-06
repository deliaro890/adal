# Usamos una imagen base de Python
FROM python:3.9-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos el archivo de dependencias (requirements.txt) al contenedor
COPY requirements.txt .

# actualizamos el pip
RUN pip install --upgrade pip

# Instalamos las dependencias necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto de la aplicación al contenedor
COPY . .

# Exponemos el puerto en el que correrá Uvicorn
EXPOSE 8000

# Comando para ejecutar Uvicorn y correr la aplicación FastAPI
CMD ["uvicorn", "infinity:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
