# Установим базовый образ
FROM golang:1.21.4

# Создадим рабочую директорию
WORKDIR /app

# Скопируем go.mod и go.sum
COPY go.mod go.sum ./

# Загрузим зависимости
RUN go mod download

# Скопируем остальные файлы
COPY . .

# Скомпилируем приложение
RUN go build -o /main.go

# Определим команду запуска
CMD ["/main.go"]