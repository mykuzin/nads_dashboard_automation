library(tidyverse)
library(googledrive)
library(googlesheets4)
library(readxl)

googledrive::drive_auth(path = Sys.getenv("NADS_DASHB_SECRET"))
gs4_auth(token = drive_token())

folder_url <- "https://drive.google.com/drive/folders/1HQMJKWvt0huqOrHWlq9HiNBcgtMuHo_B"

# перелік завантажених вхідних файлів
nads_files <- drive_ls(path = as_id(folder_url)) |> 
  filter(str_detect(name, ".xl")) |> 
  select(name) |>
  as_vector() |> unname() 

# вхідні ексель-файли трансформуються в гугл-шіти (копіюються у ту ж папку)
map(nads_files, ~{
  drive_cp(
    file = .x,
    name = paste0(str_extract(.x, ".{6}"), "_gs"),
    mime_type = drive_mime_type("spreadsheet")
  )
})

# це перелік створених гугл-шітів
nads_files_gs <- drive_ls(path = as_id(folder_url)) |> 
  filter(str_detect(name, "_gs")) |> 
  select(name) |>
  as_vector() |> 
  unname()

# функція для отримання року-кварталу з назви кожного файлу
quart_name_func <- function(raw_file_name) {
  my_year <- substring(str_extract(raw_file_name, "^20\\d{2}"), 1, 4)
  my_quart <- substring(str_extract(raw_file_name, "^20\\d{2} \\d"), 6, 6)
  my_year_quart <- paste0(my_year, " — ", my_quart, "кв" )
}

quarters_clean <- map(nads_files_gs, quart_name_func) |> as_vector()

# це id гугл-шітів, з якими далі працюватиму
nads_files_id <- drive_ls(path = as_id(folder_url)) |> 
  filter(str_detect(name, "_gs")) |> 
  select(id) |> 
  as_vector() |> unname()

# це одна велика функція, яка трансформує усі вкладки завантажених файлів
nads_data_transform_gs <- function(sheet_id, kv_y) { 
  # kv_y — це поточний рік квартал

  df_list <- list()
  
  # 1. Загальний штат. Посилається на вкладку, яка має конкретну назву
  df0 <- read_sheet(sheet_id, sheet = "штат+вакансії+декрет", skip = 1,
                    col_types = "c") |>
    rename(gov_1 = `...1`)
  
  df1 <- df0 |> filter(!is.na(gov_1)) |>
    select(c(1, 3:5)) |> 
    pivot_longer(cols = contains("за штатним"), values_to = "quant_shtat") |>
    mutate(
      cat_shtat = case_when(
        str_detect(name, "А") ~ "A",
        str_detect(name, "Б") ~ "B",
        str_detect(name, "В") ~ "V",
      )
    ) |> select(-name) |> rename(gov_shtat = gov_1)
  
  # 2. Вакантні посади
  df2 <- df0 |> filter(!is.na(gov_1)) |>
    select(c(1, 7:9)) |> 
    pivot_longer(cols = contains("вакантних"), values_to = "quant_vacant") |>
    mutate(
      cat_vacant = case_when(
        str_detect(name, "А") ~ "A",
        str_detect(name, "Б") ~ "B",
        str_detect(name, "В") ~ "V",
      )
    ) |> select(-name) |> rename(gov_vacant = gov_1)
  
  df1_2 <- cbind(df1, df2) |> select(gov_shtat_vacant = gov_shtat, 
                                     contains("quant"),
                                     cat_shtat_vacant = cat_shtat) |> 
    mutate(kv = {kv_y})
  
  df_list[[1]] <- df1_2
  
  # 3. Фактична кількість, стать, інваліди
  df3 <- read_sheet(sheet_id, sheet = "факт+інваліди",
                    col_types = "c")
  
  df3 <- df3 |> filter(!is.na(`Державні органи`)) |>
    select(c(1, 7:9, 11:14))
  
  df3_disabled <- df3  |>
    select(gov_disabled = `Державні органи`, quant_disabled = contains("інвалідн")) |> 
    mutate(kv = {kv_y})
  
  df_list[[2]] <- df3_disabled
  
  df3 <- df3 |> 
    rename_with(.fn = ~ paste0("men", "_", tolower(str_sub(.x, -2))), .cols = contains("ЧОЛОВ")) |>
    rename_with(.fn = ~ paste0("women", "_", tolower(str_sub(.x, -2))), .cols = contains("ЖІНОК")) |>
    rename_with(.fn = ~ str_replace(.x, '"', ""), .cols = contains('"')) |>
    rename(disabled = contains("інвалідн"),
           gov_fact = `Державні органи`)
  
  df3 <- df3 |> select(-disabled) |>
    pivot_longer(cols = contains("men")) |> 
    mutate(
      cat_fact = case_when(
        str_detect(name, "_а") ~ "A",
        str_detect(name, "_б") ~ "Б",
        str_detect(name, "_в") ~ "В",
      ),
      sex_fact = case_when(
        str_detect(name, regex("^men")) ~ "men",
        str_detect(name, "women") ~ "women"
      )
    ) |> select(-name) |> rename(quant_fact = value) |> 
    mutate(kv = {kv_y})
  
  df_list[[3]] <- df3
  
  # 4. Вік та стать
    # чоловіки
  df_age1 <- read_sheet(sheet_id, sheet = "розп-стать+вік (ЧОЛ) ",
                        col_types = "c") |> 
    select(-c(2:4)) |> filter(!is.na(`Державні органи`))
  
  df_age1 <- df_age1 |> rename_with(.fn = ~ paste0(c(rep("do35 ", 3), rep("36-60 ", 3), 
                                                     rep("61-64 ", 3), rep("65-70 ", 3)), .x), 
                                    .cols = c(2:13)) |>
    rename_with(.fn = ~ paste0(rep(c("men_A_", "men_B_", "men_C_")), .x), 
                .cols = c(2:13))
  
  df_age1 <- df_age1 |> pivot_longer(cols = c(2:13), values_to = "quant_age") |> 
    mutate(
      age_group = case_when(
        str_detect(name, "do35") ~ "do35",
        str_detect(name, "36-60") ~ "36-60",
        str_detect(name, "61-64") ~ "61-64",
        str_detect(name, "65-70") ~ "65-70"
      ),
      cat_age = case_when(
        str_detect(name, "_A_") ~ "A",
        str_detect(name, "_B_") ~ "B",
        str_detect(name, "_C_") ~ "V"
      ),
      sex_age = "men"
    ) |> select(-name)
  
    # жінки
  df_age2 <- read_sheet(sheet_id, sheet = "розп-стать+вік (ЖІН)",
                        col_types = "c") |> 
    select(-c(2:4)) |> filter(!is.na(`Державні органи`))
  
  df_age2 <- df_age2 |> rename_with(.fn = ~ paste0(c(rep("do35 ", 3), rep("36-60 ", 3), 
                                                     rep("61-64 ", 3), rep("65-70 ", 3)), .x), 
                                    .cols = c(2:13)) |>
    rename_with(.fn = ~ paste0(rep(c("women_A_", "women_B_", "women_C_")), .x), 
                .cols = c(2:13))
  
  df_age2 <- df_age2 |> pivot_longer(cols = c(2:13), values_to = "quant_age") |> 
    mutate(
      age_group = case_when(
        str_detect(name, "do35") ~ "do35",
        str_detect(name, "36-60") ~ "36-60",
        str_detect(name, "61-64") ~ "61-64",
        str_detect(name, "65-70") ~ "65-70"
      ),
      cat_age = case_when(
        str_detect(name, "_A_") ~ "A",
        str_detect(name, "_B_") ~ "B",
        str_detect(name, "_C_") ~ "V"
      ),
      sex_age = "women"
    ) |> select(-name)
  
  df4 <- rbind(df_age1, df_age2) |> rename(gov_age = `Державні органи`) |>
    mutate(kv = {kv_y})
  
  df_list[[4]] <- df4
  
  # 5. Декрет
  df5 <- df0 |> filter(!is.na(gov_1)) |>
    select(c(1, 10, 11)) |> 
    rename(dekret_all = `Кількість державних службовців у відпустці для догляду за дитиною`,
           dekret_men = contains("ЧОЛОВІКІВ")) |>
    mutate(dekret_women = as.numeric(dekret_all) - as.numeric(dekret_men),
           dekret_women = as.character(dekret_women)) |> 
    select(-dekret_all) |>
    pivot_longer(cols = contains("dekret"), values_to = "quant_dekret") |>
    mutate(
      sex = case_when(
        str_detect(name, "_men") ~ "male",
        str_detect(name, "women") ~ "female"
      )
    ) |> select(-name) |> rename(gov_dekret = gov_1) |> 
    mutate(kv = {kv_y})
  
  df_list[[5]] <- df5
  
  # 6. Мобілізовані
  df6 <- read_sheet(sheet_id, sheet = "мобілізовані Ч+Ж", skip = 1,
                    col_types = "c") |> 
    select(-2) |> filter(!is.na(`Державні органи`)) |>
    rename(gov_cons = `Державні органи`) |>
    pivot_longer(values_to = "quant_cons", cols = -gov_cons) |>
    mutate(
      sex_cons = case_when(
        str_detect(name, 'ЧОЛОВ') ~ "men",
        str_detect(name, 'ЖІНК') ~ "women"
      )
    ) |> select(-name) |> 
    mutate(kv = {kv_y})
  
  df_list[[6]] <- df6
  
  # 7. Закордоном
  df7 <- read_sheet(sheet_id, sheet = "за кордоном Ч+Ж",
                    col_types = "c") |> 
    select(-c(2:6, 10)) |> filter(!is.na(`Назва державного органу (повна та офіційне скорочення)`))
  
  df7 <- df7 |> pivot_longer(c(2:7), values_to = "quant_abr") |> 
    mutate(
      cat_abr = case_when(
        str_detect(name, '"А"') ~ "A",
        str_detect(name, '"Б"') ~ "Б",
        str_detect(name, '"В"') ~ "В"
      ),
      sex_abr = case_when(
        str_detect(name, 'ЧОЛОВ') ~ "men",
        str_detect(name, 'ЖІНК') ~ "women"
      )
    ) |> select(c(gov_abr = 1, 3:5)) |> 
    mutate(kv = {kv_y})
  
  df_list[[7]] <- df7
  
  # 8. Призначені та звільнені
  df_8in <- read_sheet(sheet_id, sheet = "призначені-з поч.року",
                       col_types = "c") |> 
    select(c(1, 7:9, 11:13)) |> filter(!is.na(`Державні органи`))
  
  df_8in <- df_8in |> rename_with(.fn = ~ paste0("men", "_", tolower(str_sub(.x, -6))), .cols = c(2:4)) |>
    rename_with(.fn = ~ paste0("women", "_", tolower(str_sub(.x, -7))), .cols = c(5:7)) |> 
    pivot_longer(cols = c(2:7), values_to = "quant_in_out")
  
  df_8in <- df_8in |>
    mutate(
      cat_in_out = case_when(
        str_detect(name, "_а") ~ "A",
        str_detect(name, "_б") ~ "Б",
        str_detect(name, "_в") ~ "В",
      ),
      sex_in_out = case_when(
        str_detect(name, regex("^men")) ~ "men",
        str_detect(name, "women") ~ "women"
      ),
      status_in_out = "in"
    ) |> select(-name)
  
  df_8out <- read_sheet(sheet_id, sheet = "звільн-з поч.року",
                        col_types = "c") |> 
    select(c(1, 7:9, 11:13)) |> filter(!is.na(`Державні органи`))
  
  df_8out <- df_8out |> rename_with(.fn = ~ paste0("men", "_", tolower(str_sub(.x, -6))), .cols = c(2:4)) |>
    rename_with(.fn = ~ paste0("women", "_", tolower(str_sub(.x, -7))), .cols = c(5:7)) |> 
    pivot_longer(cols = c(2:7), values_to = "quant_in_out")
  
  df_8out <- df_8out |>
    mutate(
      cat_in_out = case_when(
        str_detect(name, "_а") ~ "A",
        str_detect(name, "_б") ~ "Б",
        str_detect(name, "_в") ~ "В",
      ),
      sex_in_out = case_when(
        str_detect(name, regex("^men")) ~ "men",
        str_detect(name, "women") ~ "women"
      ),
      status_in_out = "out"
    ) |> select(-name)
  
  df8 <- rbind(df_8in, df_8out) |> rename(gov_in_out = `Державні органи`) |> 
    mutate(kv = {kv_y})
  
  df_list[[8]] <- df8
  
  # rm(df_8in, df_8out, df_age1, df_age2, df0, df1, df2)
  
  return(df_list)
}

# застосовуємо функцію: до кожного гугл-шіта, та виокремленого року-кварталу
transformed_data <- map2(nads_files_id, quarters_clean, nads_data_transform_gs)

# зі списку, де кожен елемент -  вкладки окремого гугл-шіта, роблю список, 
  # де кожен елемент - об'єднані вкладки гугл-шітів  
result_list <- map(1:8, ~ bind_rows(map(transformed_data, `[[`, .x)))

# додаю додаткою елемент до списку — назви усіх типів держ.органів

result_list[[9]] <- result_list[[1]] |> 
  distinct(gov_shtat_vacant) |> 
  rename(gov = gov_shtat_vacant)

names(result_list) <- c("shtat_vacant", "disabled", "fact", "age", "dekret", 
                        "conscript", "abroad", "in_out", "aux")

result_list <- 
map(result_list, function(df) {
  df |>
    mutate(across(contains("quant"), as.numeric),
           across(contains("quant"), ~ replace_na(., 0)))
})

result_list[[8]]$sex_in_out <- NULL # статі немає у даних за попередні роки


# видаляю технічні файли (створені гугл-шіти) з гугл-драйви
files_to_delete <- drive_ls(path = as_id(folder_url)) |>
  filter(stringr::str_detect(name, "_gs$"))

purrr::walk(files_to_delete$id, drive_rm)

# опрацьовую файл за попередні періоди, щоби до нього додати нові періоди

spreadsheet_id_prev <- "1dsAFqlW3wOhUmPtBzOfEfUfkZrvqlOD8a_uorzle280"
sheet_names_prev <- sheet_names("1dsAFqlW3wOhUmPtBzOfEfUfkZrvqlOD8a_uorzle280")

sheets_list_prev <- lapply(sheet_names_prev, function(sheet) {
  read_sheet(spreadsheet_id_prev, sheet = sheet)
})

names(sheets_list_prev) <- sheet_names_prev

# names(sheets_list_prev) == names(result_list)

# замінюю значення кварталів зі старого файлу на новий формат  

sheets_list_prev <- 
map(seq_along(sheets_list_prev), function(i) {
  if (i != 9) {
    sheets_list_prev[[i]] |>
      mutate(kv = case_when(
        kv == "kv1_24" ~ "2024 — 1кв",
        kv == "kv2_24" ~ "2024 — 2кв",
        kv == "kv3_24" ~ "2024 — 3кв",
        kv == "kv4_21" ~ "2021 — 4кв",
        TRUE ~ kv 
      ))
  } else {
    sheets_list_prev[[i]] 
  }
})

# об'єдную усі трансформовані файли в один фінальний
result <- Map(rbind, result_list, sheets_list_prev)
result[[9]] <- result[[9]] |> distinct()

# записую фінальний на гугл-диск:

  # перевірка, чи існує файл, куди усе записуватиметься
existing_file <- drive_ls(path = as_id("12FlepMOeu37Lng3emZZhvqCELGXH7B0v"), 
                          pattern = "full_updated_transformed")

if (nrow(existing_file) == 0) {
  # якщо такого файла немає, створюється новий
  result_ss <- drive_create(
    name = "full_updated_transformed",
    type = "spreadsheet",
    path = as_id("12FlepMOeu37Lng3emZZhvqCELGXH7B0v"))
} else {
  # якщо існує, використовуємо його
  result_ss <- as_id(existing_file$id[1])
}

# Перезаписуємо вкладки новим змістом
walk2(result, sheet_names_prev, function(df, sheet_name) {
  sheet_write(df, 
              ss = result_ss, 
              sheet = sheet_name)
})

# Видаляємо порожню вкладку
if ("Sheet1" %in% sheet_names(result_ss)) {
  sheet_delete(ss = result_ss, sheet = "Sheet1")
}

