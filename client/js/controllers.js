class PlotController {
    _dateInput
    _forwardButton
    _backwardButton
    _plotDiv
    _loadingOverlay
    _apiUrl
    _lastUpdateKey
    _maxWidth
    _stepSize
    _containerDiv
    _inputDiv

    _minDate = null
    _maxDate = null
    _status = null
    _lastUpdate = null

    constructor(apiUrl, containerId, lastUpdateKey, stepSize = 10, maxWidth = 1080) {
        this._apiUrl = apiUrl
        this._lastUpdateKey = lastUpdateKey
        this._stepSize = stepSize
        this._maxWidth = maxWidth

        this._containerDiv = document.getElementById(containerId)

        this._plotDiv = document.createElement("div")
        this._containerDiv.appendChild(this._plotDiv)

        this._inputDiv = document.createElement("div")
        this._inputDiv.classList.add("inputs")
        this._containerDiv.appendChild(this._inputDiv)

        this._dateInput = document.createElement("input")
        this._dateInput.type = "datetime-local"
        this._dateInput.step = toString(this._stepSize * 60)
        this._inputDiv.appendChild(this._dateInput)

        this._backwardButton = document.createElement("button")
        this._backwardButton.innerHTML = "<i class=\"fa-solid fa-backward\"></i>"
        this._inputDiv.appendChild(this._backwardButton)

        this._forwardButton = document.createElement("button")
        this._forwardButton.innerHTML = "<i class=\"fa-solid fa-forward\"></i>"
        this._inputDiv.appendChild(this._forwardButton)

        this._loadingOverlay = document.createElement("div")
        this._loadingOverlay.classList.add("loading")
        this._loadingOverlay.innerHTML = "<div class=\"spinner\"></div>"
        this._containerDiv.appendChild(this._loadingOverlay)


        // simulating abstract class/method
        if (this.display === undefined) {
            throw new TypeError("Must implement display method")
        }

        if (this.setup === undefined) {
            throw new TypeError("Must implement setup method")
        }

        if (this.updatePlot === undefined) {
            throw new TypeError("Must implement updatePlot method")
        }
    }

    // functions
    _setNavDisabled(disabled) {
        this._forwardButton.disabled = disabled
        this._backwardButton.disabled = disabled
        this._dateInput.disabled = disabled
        this._loadingOverlay.className = disabled ? "loading" : ""
    }

    async updateStatus(index) {
        // updates status if necessary, decided from index that contains lastUpdateKey and lastUpdate associated with it
        if (this._lastUpdate === index[this._lastUpdateKey]) {
            return false
        }
        this._lastUpdate = index[this._lastUpdateKey]
        this._status = await fetchData(this._apiUrl + "status")
        this.updateDateInput()
        return true
    }

    updateDateInput() {
        // update the allowed dates in the datetime-local input
        let result = calcMinMaxDate(this._status)
        this._minDate = result.minDate
        this._maxDate = result.maxDate
        // min has to be set in local time while minDate remains in UTC for comparisons
        let inMin = new Date(this._minDate)
        inMin.setHours(inMin.getHours() - 2 * inMin.getTimezoneOffset() / 60)
        this._dateInput.min = localToUtcString(inMin)
        // max has to be set in local time while maxDate remains in UTC for comparisons
        let inMax = new Date(this._maxDate)
        inMax.setHours(inMax.getHours() - 2 * inMax.getTimezoneOffset() / 60)
        this._dateInput.max = localToUtcString(inMax)
    }
}

class LinePlotController extends PlotController {
    _dataReqName
    _maxViewRange
    _minViewRange
    _plotFormat
    _viewRange

    _requestedMinDate = null
    _requestedMaxDate = null
    _data = null
    _resizeTimeout = null
    _showLegend = true

    constructor(apiUrl, containerId, lastUpdateKey, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, containerId, lastUpdateKey, stepSize, maxWidth)
        this._dataReqName = dataReqName
        this._maxViewRange = maxViewRange
        this._viewRange = this._maxViewRange
        this._minViewRange = minViewRange
        this._plotFormat = structuredClone(plotFormat)
    }

    updatePlotAndDimensions() {
        let width = window.getComputedStyle(this._plotDiv).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        width = parseInt(width)
        const part = (width - 400) / (this._maxWidth - 400)
        this._viewRange = this._minViewRange + Math.round((this._maxViewRange - this._minViewRange) * part)
        this.updatePlot()

        // legend slides into plot even after updatePlot, seems like an oversight in Plotly.react, this solves it
        if (this._plotDiv.layout !== undefined) Plotly.relayout(this._plotDiv, this._plotDiv.layout)
    }

    updatePlot(force = false) {
        // update all plots with data from datetime-local input
        let rounded = floorToMinutes(this._dateInput.value + ":00", this._stepSize)

        if (addMinutesToISODate(rounded, this._viewRange * 60) > this._maxDate) {
            rounded = addMinutesToISODate(this._maxDate, -this._viewRange * 60)
        }
        if (addMinutesToISODate(rounded, this._viewRange * 60) < this._minDate) {
            rounded = addMinutesToISODate(this._minDate, this._viewRange * 60)
        }

        this._dateInput.value = addMinutesToISODate(rounded, -getTZOffset(rounded))

        this._updateLines(rounded, force).then()
    }

    _makeLines(from, to) {
        let data = this._data.data
        let x = []
        let ys = {}

        for (let key in this._plotFormat) {
            ys[key] = []
        }

        for (let i = from; i <= to; i = addMinutesToISODate(i, this._stepSize)) {
            let item = data[i]

            // display date in local time
            let date = new Date(i)
            date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
            x.push(localToUtcString(date).replace("T", " "))
            for (let fet in this._plotFormat) {
                ys[fet].push(item[fet])
            }

        }

        let plotData = []

        let i = 0
        for (let fet in this._plotFormat) {
            let format = this._plotFormat[fet]
            plotData.push({
                type: "scatter",
                x: x,
                y: ys[fet],
                mode: "lines",
                name: format.name,
                line: {
                    dash: format.dash,
                    color: format.color,
                    width: 3
                },
                visible: this._plotDiv.data ? this._plotDiv.data[i++].visible : "true"
            })
        }

        let plotLayout = {
            font: {
                size: 16,
                color: "rgb(200, 200, 200)"
            },
            autosize: true,
            margin: {
                l: 72,
                r: 20,
                t: 20,
            },
            xaxis: {
                gridcolor: "rgb(200, 200, 200)",
            },
            yaxis: {
                gridcolor: "rgb(200, 200, 200)",
                ticksuffix: " MW",
                hoverformat: ".1f"
            },
            showlegend: this._showLegend,
            legend: {
                orientation: "h",
                xanchor: "center",
                x: 0.5
            },
            height: 700,
            paper_bgcolor: "rgba(0, 0, 0, 0)",
            plot_bgcolor: "rgba(0, 0, 0, 0)",
            hoverlabel: {
                font: {
                    size: 18,
                },
                namelength: -1,
            }
        }

        let plotConfig = {
            responsive: true,
            modeBarButtonsToRemove: [
                "pan2d",
                "zoom2d",
                "zoomIn2d",
                "zoomOut2d",
                "autoScale2d"
            ]
        }

        Plotly.react(this._plotDiv, plotData, plotLayout, plotConfig)
    }

    async _updateLines(datetime, force = false) {
        let from = addMinutesToISODate(datetime, -this._viewRange * 60)
        let to = addMinutesToISODate(datetime, this._viewRange * 60)

        let reRequest = false
        if (this._requestedMinDate === null || this._requestedMaxDate === null) {
            // setting a smaller range to reduce load times
            this._requestedMinDate = addMinutesToISODate(datetime, -this._viewRange * 2 * 60)
            this._requestedMaxDate = addMinutesToISODate(datetime, this._viewRange * 2 * 60)

            reRequest = true
        } else if (force || (from < this._requestedMinDate) || (to > this._requestedMaxDate)) {
            this._requestedMinDate = addMinutesToISODate(datetime, -this._viewRange * 4 * 60)
            this._requestedMaxDate = addMinutesToISODate(datetime, this._viewRange * 4 * 60)

            if (this._requestedMaxDate > this._maxDate) {
                this._requestedMaxDate = this._maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this._setNavDisabled(true)

            let paramStartChar = this._dataReqName.includes("?") ? "&" : "?"
            this._data = await fetchData(
                this._apiUrl + this._dataReqName + paramStartChar +
                "start_date=" + this._requestedMinDate + "&end_date=" + this._requestedMaxDate
            )

            this._setNavDisabled(false)
        }

        this._makeLines(from, to)
    }

    display() {
        this.updatePlotAndDimensions()
    }
}

class PageController {
    button
    #div
    controllers = {}

    constructor(buttonId, divId) {
        this.button = document.getElementById(buttonId)
        this.#div = document.getElementById(divId)
    }

    addController(name, ctl) {
        this.controllers[name] = ctl
    }

    async setupControllers(index) {
        for (let key in this.controllers) {
            await this.controllers[key].setup(index)
        }
    }

    async updateControllers(index) {
        // update Controllers with given index that contains lastUpdateKeys and lastUpdates
        // returns the names of updated controllers
        let updated = []
        for (let key in this.controllers) {
            if (await this.controllers[key].updateStatus(index)) {
                updated.push(key)
            }
        }
        return updated
    }

    switchAway() {
        this.#div.style.display = "none"
    }

    switchTo() {
        this.#div.style.display = "block"
        for (let key in this.controllers) {
            this.controllers[key].display()
        }
    }
}
