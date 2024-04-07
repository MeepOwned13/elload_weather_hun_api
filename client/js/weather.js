class OmszController {
    // constants
    #urlA = document.getElementById("omszUrlA")
    #mapDivId = "omszStationMapDiv"
    #loadingOverlay = document.getElementById("omszLoadingOverlay")
    #forwardButton = document.getElementById("omszForwardButton")
    #backwardButton = document.getElementById("omszBackwardButton")
    #dateInput = document.getElementById("omszDateInput")
    #dropdown = document.getElementById("omszDropdown")
    #logoImg = document.getElementById("omszLogo")
    #mapBaseLotAxis = {
        "min": 15.7,
        "max": 23.3
    } // Longitude to fit Hungary map
    #mapBaseWidth = 1080 // maximal width defined via css
    #mapHeight = 672 // adjusted for width of 1080 that is maximal in the css (1100 - 2 × 10)

    // globals variables
    #minDate = null
    #maxDate = null
    #requestedMinDate = null
    #requestedMaxDate = null
    #status = null
    #data = null
    #mapLotAxis = [this.#mapBaseLotAxis.min, this.#mapBaseLotAxis.max]
    #resizeTimeout = null

    // map formatting Object
    #mapFormat = {
        Temp: {
            name: langStringText('temp'),
            min: -10,
            max: 35,
            gradient: [
                [0, 212, 255, 1],
                [254, 255, 0, 1],
                [255, 128, 0, 1],
                [255, 0, 0, 1],
            ],
            measurement: '°C',
        },
        AvgTemp: {
            name: langStringText('avgTemp'),
            min: -10,
            max: 35,
            gradient: [
                [0, 212, 255, 1],
                [254, 255, 0, 1],
                [255, 128, 0, 1],
                [255, 0, 0, 1],
            ],
            measurement: '°C',
        },
        Prec: {
            name: langStringText('prec'),
            min: 0,
            max: 0.3,
            gradient: [
                [255, 255, 255, 0.05],
                [28, 189, 227, 1],
                [24, 0, 255, 1]
            ],
            measurement: 'mm'
        },
        RHum: {
            name: langStringText('rHum'),
            min: 0,
            max: 100,
            gradient: [
                [255, 51, 0, 1],
                [0, 255, 115, 1],
                [28, 189, 227, 1],
                [12, 0, 255, 1]
            ],
            measurement: '%'
        },
        GRad: {
            name: langStringText('gRad'),
            min: 0,
            max: 650,
            gradient: [
                [28, 189, 227, 0.5],
                [181, 208, 43, 1],
                [232, 255, 0, 1],
                [255, 179, 0, 1],
                [255, 51, 0, 1]
            ],
            measurement: 'W/m²'
        },
        AvgWS: {
            name: langStringText('avgWS'),
            min: 0,
            max: 25,
            gradient: [
                [28, 189, 227, 1],
                [0, 255, 68, 1],
                [253, 255, 0, 1],
                [255, 203, 0, 1],
                [255, 0, 0, 1]
            ],
            measurement: 'm/s',
            directionFeature: "AvgWD"
        }
    }

    // functions
    #setNavDisabled(disabled) {
        this.#dropdown.disabled = disabled
        this.#forwardButton.disabled = disabled
        this.#backwardButton.disabled = disabled
        this.#loadingOverlay.className = disabled ? "loading" : ""
    }

    async updateStatus() {
        this.#status = await fetchData(apiUrl + 'omsz/status')
    }

    #makeMap(datetime, column) {
        // Construct the stationMap
        this.#urlA.href = this.#data.Message.match(/\(([^)]+)\)/)[1]

        let status = this.#status.data
        let data = this.#data.data[datetime]
        if (data === undefined) {
            throw new Error("No data for " + datetime)
        }
        let format = this.#mapFormat[column]

        let plotData = []

        for (let key in data) {
            let item = status[key]
            let station = data[key]

            let color = null
            let value = null
            // station may be not retrieved, not have respective column or not have data for given time
            // since I'm assigning a value inside the if statement, I'll need a solution with && (cause: lazy execution)
            if (((value = station[column]) === null) || (value === undefined)) {
                continue
            }
            let interpol = linearGradient(format.gradient, getPercentageInRange(format.min, format.max, value))
            color = arrToRGBA(interpol)

            let text = value.toString() + format.measurement + ' ' + item.StationName.trim()
            let lon = item.Longitude
            let lat = item.Latitude

            let angle = 0
            let symbol = "circle"
            let size = 25

            if (format.directionFeature) {
                if (value === 0) {
                    continue // if windstrength is 0, then skip it
                }
                angle = station[format.directionFeature]
                symbol = "arrow-wide"
                size = 27
            }

            plotData.push({
                type: 'scattergeo',
                mode: 'markers',
                text: [text],
                lon: [lon],
                lat: [lat],
                hoverinfo: "text",
                marker: {
                    angle: angle,
                    angleref: "up",
                    symbol: symbol,
                    size: size,
                    color: color,
                },
                textposition: [
                    'top right', 'top left'
                ],
                hoverlabel: {
                    font: {
                        size: 20,
                    },
                    namelength: -1
                }
            })
        }

        let plotLayout = {
            font: {
                size: 20
            },
            geo: {
                scope: 'europe',
                resolution: 50,
                projection: {
                    type: 'mercator'
                },
                lonaxis: {
                    'range': this.#mapLotAxis
                },
                lataxis: {
                    'range': [45.6, 48.8]
                },
                showrivers: true,
                rivercolor: '#0c1ba3',
                riverwidth: 4,
                showlakes: true,
                lakecolor: '#0c1ba3',
                showland: true,
                showcountries: true,
                landcolor: '#0e010d00',
                countrycolor: '#e8e4c9',
                countrywidth: 3,
                subunitcolor: '#a1a1a1',
                bgcolor: '#e8e4c900',
            },
            autosize: true,
            margin: {
                l: 0,
                r: 0,
                b: 0,
                t: 0,
            },
            height: this.#mapHeight,
            showlegend: false,
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
        }

        let plotConfig = {
            responsive: true,
            modeBarButtonsToRemove: [
                'select2d',
                'lasso2d'
            ]
        }

        Plotly.newPlot(this.#mapDivId, plotData, plotLayout, plotConfig);
    }

    async #updateMap(datetime, column) {
        // update of map on given datetime, requests data on its own
        if (this.#status === null) {
            await this.updateStatus()
        }

        let reRequest = false
        if (this.#requestedMinDate === null || this.#requestedMaxDate === null) {
            this.#requestedMaxDate = datetime // first request is always current time
            // let's set it 1 hour back for the first time to reduce traffic
            this.#requestedMinDate = addHoursToISODate(datetime, -1)

            reRequest = true
        } else if (!validDate(datetime, this.#requestedMinDate, this.#requestedMaxDate)) {
            this.#requestedMinDate = addHoursToISODate(datetime, -3)
            this.#requestedMaxDate = addHoursToISODate(datetime, 3)
            if (this.#requestedMaxDate > this.#maxDate) {
                this.#requestedMaxDate = this.#maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this.#setNavDisabled(true)

            let cols = []
            for (let key in this.#mapFormat) {
                cols.push(key)
                if (this.#mapFormat[key].directionFeature) {
                    cols.push(this.#mapFormat[key].directionFeature)
                }
            }

            this.#data = await fetchData(
                apiUrl + 'omsz/weather?start_date=' + this.#requestedMinDate + '&end_date=' + this.#requestedMaxDate +
                '&date_first=True&col=' + cols.join('&col=')
            )

            this.#setNavDisabled(false)
        }

        this.#makeMap(datetime, column)
    }

    updatePlot() {
        // update all plots with data from datetime-local input
        let rounded = floorTo10Min(this.#dateInput.value + ":00")
        if (!validDate(localToUtcString(rounded), this.#minDate, this.#maxDate)) {
            rounded = new Date(this.#maxDate)
            rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
        }

        // Return to local time to set the element, and then back to utc
        rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
        this.#dateInput.value = localToUtcString(rounded)
        rounded.setHours(rounded.getHours() + rounded.getTimezoneOffset() / 60)

        let datetime = localToUtcString(rounded)

        let column = this.#dropdown.value
        if (!(column in this.#mapFormat)) {
            throw new Error("Selected option (" + column + ") unavailable")
        }

        this.#updateMap(datetime, column).then()
    }

    update() {
        // update elements
        let result = calcMinMaxDate(this.#status)
        this.#minDate = result.minDate
        this.#maxDate = result.maxDate
        // min has to be set in local time while minDate remains in UTC for comparisons
        let inMin = new Date(this.#minDate)
        inMin.setHours(inMin.getHours() - 2 * inMin.getTimezoneOffset() / 60)
        this.#dateInput.min = localToUtcString(inMin)
        // max has to be set in local time while maxDate remains in UTC for comparisons
        let inMax = new Date(this.#maxDate)
        inMax.setHours(inMax.getHours() - 2 * inMax.getTimezoneOffset() / 60)
        this.#dateInput.max = localToUtcString(inMax)
    }

    updateMapDimensions() {
        const width = window.getComputedStyle(document.getElementById(this.#mapDivId)).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        const part = width / this.#mapBaseWidth
        const newLotRange = (this.#mapBaseLotAxis.max - this.#mapBaseLotAxis.min) * part
        const centerLot = (this.#mapBaseLotAxis.max + this.#mapBaseLotAxis.min) / 2
        this.#mapLotAxis[0] = centerLot - newLotRange / 2
        this.#mapLotAxis[1] = centerLot + newLotRange / 2
    }

    // construct elements
    async setup() {
        // setup function, assumes that status is set
        await this.updateStatus()
        this.update()
        this.#dateInput.value = this.#dateInput.max

        let dropdownOptions = []
        for (let key in this.#mapFormat) {
            dropdownOptions.push(
                '<option value="' + key + '">' + this.#mapFormat[key].name + '</option>'
            )
        }
        this.#dropdown.innerHTML = dropdownOptions.join('\n')

        fetchData(apiUrl + 'omsz/logo').then((resp) => {
            this.#logoImg.src = resp
        })

        this.updateMapDimensions()
        this.updatePlot()

        this.#dateInput.addEventListener("change", () => {
            this.updatePlot()
        })
        this.#dropdown.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this.#forwardButton, () => {
            addMinutesToInputRounded10(this.#dateInput, 10)
            this.updatePlot()
        }, 200, "omszForward")

        addIntervalToButton(this.#backwardButton, () => {
            addMinutesToInputRounded10(this.#dateInput, -10)
            this.updatePlot()
        }, 200, "omszBackward")

        window.addEventListener('resize', () => {
            clearTimeout(this.#resizeTimeout)
            this.#resizeTimeout = setTimeout(() => {
                this.updateMapDimensions()
            }, 50)
        })
    }

    switch() {
        this.update()
        this.updateMapDimensions()
        this.updatePlot()
    }
}
