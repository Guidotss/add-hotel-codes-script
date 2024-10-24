#include <iostream>
#include <string>
#include <vector>
#include <curl/curl.h>
#include <fstream>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include "include/json.hpp"
#include <unordered_map>

using json = nlohmann::json;
using namespace std;

void printAndFlush(const std::string& message) {
    std::cout << message << std::endl;
    std::cout.flush();
}


std::mutex mtx;
std::condition_variable cv;
std::queue<std::string> cityCodeQueue;
bool finished = false;

json globalResponseJson;

std::vector<std::thread> threads;
std::vector<CURL*> curlHandles;

size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

struct HotelData {
    std::vector<std::string> hotelCodes;
    double cityLatitude;
    double cityLongitude;
};

void saveJsonToFile(const json& j, const std::string& filename) {
    std::lock_guard<std::mutex> lock(mtx);
    std::ofstream file(filename, std::ios::app);
    if (file.is_open()) {
        file << j.dump() << std::endl;
        std::cout << "Datos guardados en " << filename << ": " << j.dump() << std::endl;
    } else {
        std::cerr << "No se pudo abrir el archivo para escribir: " << filename << std::endl;
    }
}

HotelData fetchHotelCodesAndFirstCoordinates(CURL* curl, const std::string& cityCode) {
    std::cout << "Iniciando fetchHotelCodesAndFirstCoordinates para cityCode: " << cityCode << std::endl;
    CURLcode res;
    std::string readBuffer;
    HotelData result = { {}, 0.0, 0.0 };

    const std::string API_USERNAME = "";
    const std::string API_PASSWORD = "";
    const std::string API_URL = "";

    curl_easy_setopt(curl, CURLOPT_URL, API_URL.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERNAME, API_USERNAME.c_str());
    curl_easy_setopt(curl, CURLOPT_PASSWORD, API_PASSWORD.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json requestBody = {
        {"CityCode", cityCode}, 
        {"IsDetailedResponse", false}
    };
    std::string requestBodyStr = requestBody.dump();
    std::cout << "Request Body: " << requestBodyStr << std::endl;
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, requestBodyStr.c_str());

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    const int MAX_RETRIES = 3;
    for (int retry = 0; retry < MAX_RETRIES; ++retry) {
        res = curl_easy_perform(curl);
        if (res == CURLE_OK) {
            break;
        }
        std::cerr << "Intento " << retry + 1 << " fallido para cityCode " << cityCode << ": " << curl_easy_strerror(res) << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2 * (retry + 1)));
    }

    if (res != CURLE_OK) {
        std::cerr << "Error en la petición después de " << MAX_RETRIES << " intentos: " << curl_easy_strerror(res) << std::endl;
        return result;
    }
    
    std::cout << "Respuesta del servidor: " << readBuffer << std::endl;

    if (res != CURLE_OK) {
        std::cerr << "Error en la petición: " << curl_easy_strerror(res) << std::endl;
    } else {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (http_code != 200) {
            std::cerr << "Error HTTP: " << http_code << std::endl;
            std::cerr << "Respuesta del servidor: " << readBuffer << std::endl;
        } else {
            if (readBuffer.empty()) {
                std::cerr << "La respuesta del servidor está vacía para cityCode: " << cityCode << std::endl;
                return result;
            }
            try {
                json responseJson = json::parse(readBuffer);
                if (responseJson["Status"]["Code"] == 200) {
                    json outputJson;
                    outputJson["hotelCodes"] = json::array();
                    
                    if (responseJson.contains("Hotels") && !responseJson["Hotels"].empty()) {
                        outputJson["cityName"] = responseJson["Hotels"][0]["CityName"]; 
                        outputJson["cityCode"] = cityCode;
                        for (const auto& hotel : responseJson["Hotels"]) {
                            outputJson["hotelCodes"].push_back(hotel["HotelCode"]);
                            result.hotelCodes.push_back(hotel["HotelCode"]);
                        }
                        
                        const auto& firstHotel = responseJson["Hotels"][0];
                        if (firstHotel.contains("Latitude") && firstHotel["Latitude"].is_string()) {
                            result.cityLatitude = std::stod(firstHotel["Latitude"].get<std::string>());
                        }
                        if (firstHotel.contains("Longitude") && firstHotel["Longitude"].is_string()) {
                            result.cityLongitude = std::stod(firstHotel["Longitude"].get<std::string>());
                        }
                    } else {
                        std::cout << "No se encontraron hoteles en la respuesta JSON." << std::endl;
                    }
                    
                    saveJsonToFile(outputJson, "city_and_hotels.json");
                } else {
                    std::cerr << "Error en la respuesta: " << responseJson["Status"]["Description"] << std::endl;
                }
                
            } catch (json::parse_error& e) {
                std::cerr << "Error al parsear JSON: " << e.what() << std::endl;
                std::cerr << "Respuesta del servidor: " << readBuffer << std::endl;
            } catch (std::exception& e) {
                std::cerr << "Error al procesar los datos: " << e.what() << std::endl;
            }
        }
    }

    std::cout << "Finalizando fetchHotelCodesAndFirstCoordinates para cityCode: " << cityCode << std::endl;
    return result;
}

void workerThread(CURL* curl) {
    printAndFlush("Iniciando workerThread");
    while (true) {
        std::string cityCode;
        {
            std::unique_lock<std::mutex> lock(mtx);
            printAndFlush("Worker esperando por un código de ciudad");
            cv.wait(lock, [] { return !cityCodeQueue.empty() || finished; });
            if (finished && cityCodeQueue.empty()) {
                printAndFlush("workerThread terminando porque finished=true y la cola está vacía");
                return;
            }
            if (!cityCodeQueue.empty()) {
                cityCode = cityCodeQueue.front();
                cityCodeQueue.pop();
                printAndFlush("Worker obtuvo el código de ciudad: " + cityCode);
            } else {
                printAndFlush("Worker despertó pero la cola está vacía");
                continue;
            }
        }

        std::cout << "Procesando cityCode: " << cityCode << std::endl;

        try {
            std::cout << "Llamando a fetchHotelCodesAndFirstCoordinates para cityCode: " << cityCode << std::endl;
            HotelData data = fetchHotelCodesAndFirstCoordinates(curl, cityCode);

            json outputJson;
            outputJson["cityCode"] = cityCode;
            outputJson["hotelCodes"] = data.hotelCodes;
            outputJson["latitude"] = data.cityLatitude;
            outputJson["longitude"] = data.cityLongitude;

            std::cout << "Guardando resultados para cityCode: " << cityCode << std::endl;
            saveJsonToFile(outputJson, "results.json");
        } catch (const std::exception& e) {
            std::cerr << "Error en workerThread para cityCode " << cityCode << ": " << e.what() << std::endl;
        }
    }
}


void addCommaAfterEachObjectOfJsonFile(const std::string& inputFilePath, const std::string& outputFilePath) {
    std::ifstream inputFile(inputFilePath);
    std::ofstream outputFile(outputFilePath);
    std::string line;
    while (std::getline(inputFile, line)) {
        outputFile << line << "," << std::endl;
    }
}


void mergeJsonFiles(const std::string& cityFilePath, const std::string& resultsFilePath, const std::string& outputFilePath);
int main() {
    curl_global_init(CURL_GLOBAL_ALL);

    std::ifstream file("saas.CityTBO.json");
    if (!file.is_open()) {
        std::cerr << "No se pudo abrir el archivo saas.CityTBO.json" << std::endl;
        return 1;
    }

    json j;
    try {
        file >> j;
        printAndFlush("Archivo JSON leído correctamente");
    } catch (const json::parse_error& e) {
        std::cerr << "Error al parsear el archivo JSON: " << e.what() << std::endl;
        return 1;
    }

    printAndFlush("Muestra del JSON: " + j.dump().substr(0, 1000)); 

    printAndFlush("Cargando códigos de ciudad...");
    int codesAdded = 0;

    if (j.is_array()) {
        for (const auto& item : j) {
            if (item.contains("code") && item["code"].is_string()) {
                std::string cityCode = item["code"].get<std::string>();
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    cityCodeQueue.push(cityCode);
                }
                codesAdded++;
                if (codesAdded % 100 == 0) {
                    printAndFlush("Añadidos " + std::to_string(codesAdded) + " códigos de ciudad");
                }
            }
        }
    } else {
        printAndFlush("El JSON no es un array como se esperaba");
    }

    printAndFlush("Total de códigos de ciudad añadidos: " + std::to_string(codesAdded));

    if (codesAdded == 0) {
        printAndFlush("No se encontraron códigos de ciudad válidos en el archivo JSON");
    }

    cv.notify_all();

    std::cout << "Todos los códigos de ciudad añadidos a la cola" << std::endl;

    printAndFlush("Tamaño de la cola antes de terminar: " + std::to_string(cityCodeQueue.size()));

    finished = true;
    cv.notify_all();

    const int NUM_THREADS = 4;
    for (int i = 0; i < NUM_THREADS; ++i) {
        CURL* curl = curl_easy_init();
        curlHandles.push_back(curl);
        threads.emplace_back(workerThread, curl);
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (auto& handle : curlHandles) {
        curl_easy_cleanup(handle);
    }

    curl_global_cleanup();


    // MANQUEADA XD 
    /* std::cout << "Añadiendo comas después de cada objeto en results.json" << std::endl;
    addCommaAfterEachObjectOfJsonFile("results.json", "results_with_commas.json");
    std::cout << "Añadiendo comas después de cada objeto en city_and_hotels.json" << std::endl;
    addCommaAfterEachObjectOfJsonFile("city_and_hotels.json", "city_and_hotels_with_commas.json"); */
    //mergeJsonFiles("saas.CityTBO.json", "results.json", "merged_results.json");

    return 0;
}

void mergeJsonFiles(const std::string& cityFilePath, const std::string& resultsFilePath, const std::string& outputFilePath) {
    std::ifstream cityFile(cityFilePath);
    std::ifstream resultsFile(resultsFilePath);
    std::ofstream outputFile(outputFilePath);

    if (!cityFile.is_open() || !resultsFile.is_open()) {
        std::cerr << "No se pudo abrir uno de los archivos de entrada." << std::endl;
        return;
    }

    json cityJson, resultsJson;
    try {
        cityFile >> cityJson;
        resultsFile >> resultsJson;
    } catch (const json::parse_error& e) {
        std::cerr << "Error al parsear uno de los archivos JSON: " << e.what() << std::endl;
        return;
    }

    for (auto& result : resultsJson) {
        std::string cityCode = result["cityCode"];
        for (const auto& city : cityJson) {
            if (city["code"] == cityCode) {
                result["cityName"] = city["name"];
                result["country"] = city["country"];
                break;
            }
        }
    }

    outputFile << resultsJson.dump(4) << std::endl;
}
