# Подключение библиотек
library(httr)
library(jsonlite)
library(dplyr)
library(DBI)
library(RPostgres)

# Переменные из GitHub Secrets 
host     <- Sys.getenv("DB_HOST")
user     <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")


# Функция запроса данных о погоде
ather_data <- function(host, user, password) {
  
  # Подключение к базе данных
  con <- dbConnect(
    Postgres(),
    dbname = "postgres",
    host = host,
    port = 5432,
    user = user,
    password = password,
    sslmode = "require"
  )
  
  # Гарантированное отключение от БД при выходе из функции
  on.exit(dbDisconnect(con))
  
  # Имя таблицы в базе данных
  weather_table <- "weather_table"
  
  # Определяем последнюю дату в базе
  last_date <- dbGetQuery(con, paste0("SELECT MAX(time) as max_time FROM ", weather_table))$max_time
  
  # Преобразуем в дату (если в секундах)
  last_date_day <- if (!is.na(last_date)) {
    as.Date(as.POSIXct(as.numeric(last_date), origin = "1970-01-01", tz = "UTC"))
  } else {
    NA
  }
  
  # Дата, за которую нужно скачать данные — всегда вчера
  download_date <- Sys.Date() - 1
  start_date <- download_date
  end_date <- download_date
  
  # Если данные за вчера уже есть — ничего не делаем
  if (!is.na(last_date_day) && last_date_day >= download_date) {
    message("Данные за вчера уже загружены")
    return(NULL)
  }
  
  message("Загрузка данных за период: ", start_date, " - ", end_date)
  
  # Датафреймы с координатами
  coords_1 <- data.frame(
    lon = c(47.301971691654785, 47.30675396654942, 47.30723791502257,
            47.31674838859459, 47.33112442562222, 47.34234948862576,
            47.369112008139496, 47.378582945649555, 47.39662248798692,
            47.42386327170445, 47.44349630943728, 47.453516074214406,
            47.45983453632985),
    lat = c(56.0301436050417, 56.02547376534528, 56.017265901241444,
            56.008658647294226, 55.99869308525135, 55.99119123834643,
            55.96708556224547, 55.94405018405632, 55.929005147878655,
            55.910500138146915, 55.88774990320931, 55.87888512305787,
            55.86757859930401),
    route_name = "Цивильск_Кугеси"
  )
  
  coords_2 <- data.frame(
    lon = c(47.460005103607415, 47.448613989693456, 47.43662926415416,
            47.415053613905826, 47.410976912949536, 47.43906169042896,
            47.43710440257783, 47.44525461279875, 47.425241017073006,
            47.423207041817165, 47.40227725157828, 47.43518565079762,
            47.45454322793154),
    lat = c(55.85312027637403, 55.81891144048657, 55.803935536870256,
            55.78974252583322, 55.78127231022518, 55.76635695597702,
            55.699622912711845, 55.65170927652807, 55.62284898737309,
            55.59294865533212, 55.55863029399609, 55.539169386897356,
            55.52101454908029),
    route_name = "Цивильск_Канаш"
  )
  
  coords_3 <- data.frame(
    lon = c(47.47520593686875, 47.50202577634701, 47.52987727398224,
            47.5520431036561, 47.580481395650764, 47.625497864816055,
            47.71598681259195, 47.83275232660367, 47.96424707929518,
            48.0043297627459, 48.0428585949968, 48.10098376481017,
            48.149370514666714),
    lat = c(55.85314279316492, 55.85111658957837, 55.84243077324206,
            55.829008137801395, 55.81129078483929, 55.80374258919326,
            55.78861697896215, 55.79554393927333, 55.80647177024454,
            55.810372096430115, 55.806246375432636, 55.79692771505137,
            55.786751846376376),
    route_name = "Цивильск_Козловка"
  )
  
  # Объединение всех координат
  coords_df <- bind_rows(coords_1, coords_2, coords_3)
  
  # Создание пустого списка для хранения результатов
  all_weather <- list()
  
  # Цикл с запросом для каждой точки
  for (i in 1:nrow(coords_df)) {
    url <- paste0(
      "https://api.open-meteo.com/v1/forecast?",
      "latitude=", coords_df$lat[i],
      "&longitude=", coords_df$lon[i],
      "&hourly=temperature_2m,relativehumidity_2m,visibility,weathercode,precipitation,windspeed_10m",
      "&start_date=", start_date,
      "&end_date=", end_date,
      "&timezone=Europe/Moscow"
    )
    
    # Выполняем запрос
    res <- GET(url)
    
    # Проверка на успешность запроса
    if(status_code(res) == 200) {
      cat("Запрос прошел успешно для точки ", i, "/", nrow(coords_df), "\n")
      data <- fromJSON(content(res, as = "text", encoding = "UTF-8"))
      
      # Создаем датафрейм с погодными данными
      df <- data.frame(
        route_name = coords_df$route_name[i],
        lon = coords_df$lon[i],
        lat = coords_df$lat[i],
        time = as.POSIXct(data$hourly$time, format = "%Y-%m-%dT%H:%M", tz = "UTC"),
        temperature = data$hourly$temperature_2m,
        relativehumidity_2m = data$hourly$relativehumidity_2m,
        visibility = data$hourly$visibility,
        weathercode = data$hourly$weathercode,
        precipitation = data$hourly$precipitation,
        windspeed_10m = data$hourly$windspeed_10m,
        stringsAsFactors = FALSE
      )
      
      # Добавляем датафрейм в список
      all_weather[[i]] <- df
      
    } else {
      warning("Ошибка запроса для точки: ", coords_df$lat[i], ", ", coords_df$lon[i], 
              " Код: ", status_code(res))
    }
    
    # Пауза чтобы не перегружать сервер
    Sys.sleep(1)
  }
  
  # Объединяем все точки в один датафрейм
  all_weather_df <- bind_rows(all_weather)
  
  # Отправка данных в базу
  if(nrow(all_weather_df) > 0) {
    dbWriteTable(con, weather_table, all_weather_df, append = TRUE, row.names = FALSE)
    message("Успешно загружено ", nrow(all_weather_df), " записей в базу данных")
  } else {
    message("Нет данных для загрузки")
  }
  
  # Возврат датафрейма
  return(all_weather_df)
}

# Вызов функции
ather_data(host, user, password)