# Подключение библиотек
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(purrr)
library(DBI)
library(RPostgres)
library(sf)

# Переменные из GitHub Secrets 
API_KEY  <- Sys.getenv("API_KEY")
host     <- Sys.getenv("DB_HOST")
user     <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")

# Функция запроса ДТП с разворачиванием линий в уникальные точки
get_incidents <- function(api_key) {
  
  # Список городов с bbox
  cities <- data.frame(
    city = c("Цивильск_Козловка_m7", "Цивильск_Канаш_a151", "Цивильск_кугеси_m7", "Чебоксары", "Новочебоксарск", "Шумерля", "Канаш", "Алатырь", 
             "Цивильск", "Мариинский Посад", "Козловка", "Ядрин", "Кугеси"),
    min_lon = c(47.50, 47.39, 47.30, 47.10, 47.40, 46.35, 47.40, 46.50, 47.40, 47.65, 48.15, 46.60, 47.25),
    min_lat = c(55.76, 55.51, 55.90, 56.05, 56.05, 55.45, 55.45, 54.75, 55.83, 56.05, 55.80, 55.88, 55.92),
    max_lon = c(48.17, 47.47, 47.50, 47.40, 47.60, 46.65, 47.60, 47.10, 47.55, 47.80, 48.35, 46.80, 47.35),
    max_lat = c(55.85, 55.84, 56.02, 56.20, 56.15, 55.55, 55.55, 54.90, 55.92, 56.20, 55.90, 56.00, 56.03)
  )
  
  all_incidents <- list()
  
  for (i in 1:nrow(cities)) {
    cat("Запрашиваем:", cities$city[i], "\n")
    
    bbox_str <- paste(cities$min_lon[i], cities$min_lat[i], 
                      cities$max_lon[i], cities$max_lat[i], sep = ",")
    
    url <- "https://api.tomtom.com/traffic/services/5/incidentDetails"
    
    res <- GET(url, query = list(
      key = api_key,
      bbox = bbox_str,
      fields = "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,startTime,endTime,events{description,code}}}}",
      categoryFilter = "0,1,2,3,4,5,6,7,8,9,10,11,14",
      timeValidityFilter = "present",
      language = "en-GB"
    ))
    
    if (status_code(res) == 200) {
      data <- fromJSON(content(res, as = "text", encoding = "UTF-8"), flatten = TRUE)
      
      if (!is.null(data$incidents) && length(data$incidents) > 0) {
        cat("Данные получены для", cities$city[i], "-", nrow(data$incidents), "инцидентов\n")
        
        df <- as.data.frame(data$incidents)
        df$city <- cities$city[i]
        
        if ("properties.events" %in% names(df)) {
          df <- df %>% unnest(properties.events)
        }
        
        # разбор координат 
        df_points <- df %>%
          rowwise() %>%
          mutate(
            coords_raw = list({
              cr <- geometry.coordinates
              
              if (is.matrix(cr)) {
                # матрица - каждая строка это [lon, lat]
                tibble(
                  lon = as.numeric(cr[, 1]),  
                  lat = as.numeric(cr[, 2])  
                )
                
              } else if (is.list(cr)) {
                # список [[lon, lat], …]
                bind_rows(lapply(cr, function(x) {
                  tibble(lon = x[[1]], lat = x[[2]])
                }))
                
              } else {
                tibble(lon = NA, lat = NA)
              }
            }),
            # считаем midpoint
            lon = mean(coords_raw$lon, na.rm = TRUE),
            lat = mean(coords_raw$lat, na.rm = TRUE),
            event_datetime = as.POSIXct(properties.startTime, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
          ) %>%
          ungroup() %>%
          select(-geometry.type,
                 -geometry.coordinates,
                 -coords_raw,
                 -properties.endTime,
                 -properties.startTime,
                 -code,
                 -properties.iconCategory) %>% 
          rename(properties_id = properties.id)
        
        all_incidents[[length(all_incidents)+1]] <- df_points
        
      } else {
        cat("Нет данных для города:", cities$city[i], "\n")
      }
      
    } else {
      warning("Ошибка запроса для города: ", cities$city[i], " - ", status_code(res), " - ", content(res, as = "text"))
    }
    
    Sys.sleep(1)
  }
  
  # Объединяем и оставляем уникальные точки для анализа
  all_incidents_df <- bind_rows(all_incidents) %>%
    distinct(properties_id, .keep_all = TRUE)
  
  if (nrow(all_incidents_df) > 0) {
    cat("\n Всего точек для анализа:", nrow(all_incidents_df), "\n")
    
    # Проверка координат
    problem_points <- all_incidents_df %>% 
      filter(round(lon, 6) == round(lat, 6) | is.na(lon) | is.na(lat))
    
    if (nrow(problem_points) > 0) {
      cat("Найдено проблемных точек:", nrow(problem_points), "\n")
    } else {
      cat("Все координаты корректны\n")
    }
    
  } else {
    cat("\n️ Инциденты не найдены\n")
  }
  
  # Оставляем только уникальные координаты и добавляем geom
  all_incidents_df <- all_incidents_df %>%
    distinct(lon, lat, .keep_all = TRUE) %>%
    rowwise() %>%
    mutate(geom = st_sfc(st_point(c(lon, lat)), crs = 4326)) %>%
    ungroup()
  
  return(all_incidents_df)
}

# Вызов функции
incidents_df <- get_incidents(API_KEY)




# Функция запроса информации о дороге исходя из координат ДТП
road_info <- function(data) {
  
  if(nrow(data) > 0) {
    cat("Данные присутствуют!\n")
    
    # Создаем пустой список
    coords <- list()
    
    # Цикл проходит по каждой строке и делает запрос в OSM
    for (i in 1:nrow(data)) {
      
      query <- paste0(
        '[out:json][timeout:120];',
        'way(around:', 50, ',', data$lat[i], ',', data$lon[i], ')["highway"]; out geom tags;'
      )
      
      url <- "https://overpass.kumi.systems/api/interpreter"
      res <- POST(url, body = list(data = query), encode = "form")
      text_res <- content(res, "text", encoding = "UTF-8")
      
      # Проверяем, что вернулся JSON
      if(substr(text_res, 1, 1) != "{"){
        message("Сервер вернул не JSON для точки ", i)
        coords[[i]] <- c(data[i, ], highway = NA, lanes = NA, maxspeed = NA, surface = NA)
        next
      }
      
      data_json <- fromJSON(text_res)
      coord_point <- data[i, ]  
      
      # Проверяем наличие дорог
      if(length(data_json$elements) == 0){
        message("Дорога не найдена для точки ", i)
        coords[[i]] <- c(data[i, ], highway = NA, lanes = NA, maxspeed = NA, surface = NA)
        next
      }
      
       # Берем первую дорогу и теги
      elem <- data_json$elements[[1]]  
      tags <- elem$tags                
      
      # Берем значения тегов безопасно
      coords[[i]] <- tibble(
        type = coord_point$type,
        properties_id = coord_point$properties_id,
        description = coord_point$description,
        city = coord_point$city,
        lon = coord_point$lon,
        lat = coord_point$lat,
        event_datetime = coord_point$event_datetime,
        geom = coord_point$geom,
        highway = if(!is.null(tags[["highway"]])) tags[["highway"]] else NA_character_,
        lanes = if(!is.null(tags[["lanes"]])) as.integer(tags[["lanes"]]) else NA_integer_,
        surface = if(!is.null(tags[["surface"]])) tags[["surface"]] else NA_character_,
        maxspeed = if(!is.null(tags[["maxspeed"]])) as.integer(tags[["maxspeed"]]) else NA_integer_
      )
      
      Sys.sleep(1)
    }
    
    # Преобразуем список в data.frame и возвращаем результат
    result <- bind_rows(coords)
    return(result)
    
  } else {
    warning("Данные отсутствуют!")
    return(NULL)
  }
}

# Вызов функции
roads_data <- road_info(incidents_df)



# Функция с обработкой дубликатов ДТП
insert_incidents <- function(data, host, user, password) {
  con <- dbConnect(
    Postgres(),
    dbname = "postgres",
    host = host,
    port = 5432,
    user = user,
    password = password,
    sslmode = "require"
  )
  
  on.exit(dbDisconnect(con))
  
  for(i in 1:nrow(data)) {
    row <- data[i, ]
    
    highway_val <- ifelse(is.na(row$highway), "NULL", paste0("'", row$highway, "'"))
    maxspeed_val <- ifelse(is.na(row$maxspeed), "NULL", paste0("'", row$maxspeed, "'"))
    surface_val <- ifelse(is.na(row$surface), "NULL", paste0("'", row$surface, "'"))
    
    sql <- sprintf("
  INSERT INTO incidents_table (
    type, properties_id, description, city, event_datetime,
    lon, lat, highway, lanes, maxspeed, surface, geom)
  VALUES ('%s','%s','%s','%s','%s',%f,%f,%s,%s,%s,%s,
  ST_SetSRID(ST_MakePoint(%f,%f),4326))
  ON CONFLICT (properties_id) DO NOTHING;
",
                   row$type, row$properties_id,
                   ifelse(is.na(row$description), "No description", row$description),
                   ifelse(is.na(row$city), "Unknown", row$city),
                   format(row$event_datetime, "%Y-%m-%d %H:%M:%S"),
                   row$lon, row$lat,
                   ifelse(is.na(row$highway), "NULL", paste0("'", row$highway, "'")),
                   ifelse(is.na(row$lanes), "NULL", row$lanes),
                   ifelse(is.na(row$maxspeed), "NULL", paste0("'", row$maxspeed, "'")),
                   ifelse(is.na(row$surface), "NULL", paste0("'", row$surface, "'")),
                   row$lon, row$lat)
    
    dbExecute(con, sql)
  }
  
  message("Вставка завершена")
}

# Вызов функции
insert_incidents(roads_data, host, user, password)



