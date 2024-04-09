class MavirController extends PlotController {
    // constants
    #legendCheckbox = document.getElementById("mavirShowLegend")
    #urlA = document.getElementById("mavirUrlA")
    #logoImg = document.getElementById("mavirLogo")
    #plotDiv = document.getElementById("mavirPlotDiv")
    #plotBaseWidth = 1080 // maximal width defined via css
    #baseViewRange = 6
    #minViewRange = 2
    #plotFormat

    // variables
    #requestedMinDate = null
    #requestedMaxDate = null
    #data = null
    #viewRange = this.#baseViewRange
    #resizeTimeout = null
    #showLegend = true

    constructor(apiUrl, lastUpdateKey, dateInputId, forwardButtonId, backwardButtonId, loadingOverlayId, plotFormat) {
        super(apiUrl, lastUpdateKey, dateInputId, forwardButtonId, backwardButtonId, loadingOverlayId)
        this.#plotFormat = structuredClone(plotFormat)
    }

    // functions
    #makeLines(from, to) {
        // update mavir lineplot with given range, expects: from < to
        this.#urlA.href = this.#data.Message.match(/\(([^)]+)\)/)[1]

        let data = this.#data.data
        let x = []
        let ys = {}

        for (let key in this.#plotFormat) {
            ys[key] = []
        }

        for (let i = from; i <= to; i = addMinutesToISODate(i, 10)) {
            let item = data[i]

            // display date in local time
            let date = new Date(i)
            date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
            x.push(localToUtcString(date).replace('T', ' '))
            for (let fet in this.#plotFormat) {
                ys[fet].push(item[fet])
            }

        }

        let plotData = []

        let i = 0
        for (let fet in this.#plotFormat) {
            let format = this.#plotFormat[fet]
            plotData.push({
                type: 'scatter',
                x: x,
                y: ys[fet],
                mode: 'lines',
                name: format.name,
                line: {
                    dash: format.dash,
                    color: format.color,
                    width: 3
                },
                visible: this.#plotDiv.data ? this.#plotDiv.data[i++].visible : "true"
            })
        }

        let plotLayout = {
            font: {
                size: 16,
                color: 'rgb(200, 200, 200)'
            },
            autosize: true,
            margin: {
                l: 72,
                r: 20,
                t: 20,
            },
            xaxis: {
                gridcolor: 'rgb(200, 200, 200)',
            },
            yaxis: {
                gridcolor: 'rgb(200, 200, 200)',
                ticksuffix: ' MW',
                hoverformat: '.1f'
            },
            showlegend: this.#showLegend,
            legend: {
                orientation: 'h',
                xanchor: 'center',
                x: 0.5
            },
            height: 700,
            paper_bgcolor: 'rgba(0, 0, 0, 0)',
            plot_bgcolor: 'rgba(0, 0, 0, 0)',
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
                'pan2d',
                'zoom2d',
                'zoomIn2d',
                'zoomOut2d',
                'autoScale2d'
            ]
        }

        Plotly.react(this.#plotDiv, plotData, plotLayout, plotConfig)
    }

    async #updateLines(datetime, force = false) {
        // update elload centered on given datetime
        let from = addHoursToISODate(datetime, -this.#viewRange)
        let to = addHoursToISODate(datetime, this.#viewRange)

        let reRequest = false
        if (this.#requestedMinDate === null || this.#requestedMaxDate === null) {
            // setting a smaller range to reduce load times
            this.#requestedMinDate = addHoursToISODate(datetime, -this.#viewRange * 2)
            this.#requestedMaxDate = addHoursToISODate(datetime, this.#viewRange * 2)

            reRequest = true
        } else if (force || (from < this.#requestedMinDate) || (to > this.#requestedMaxDate)) {
            this.#requestedMinDate = addHoursToISODate(datetime, -this.#viewRange * 4)
            this.#requestedMaxDate = addHoursToISODate(datetime, this.#viewRange * 4)

            if (this.#requestedMaxDate > this._maxDate) {
                this.#requestedMaxDate = this._maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this._setNavDisabled(true)

            this.#data = await fetchData(
                this._apiUrl + "load?start_date=" + this.#requestedMinDate + "&end_date=" + this.#requestedMaxDate
            )

            this._setNavDisabled(false)
        }

        this.#makeLines(from, to)
    }

    updatePlot(force = false) {
        // update all plots with data from datetime-local input
        let rounded = floorTo10Min(this._dateInput.value + ":00")
        
        if (addHoursToISODate(rounded, this.#viewRange) > this._maxDate) {
            rounded = addHoursToISODate(this._maxDate, -this.#viewRange)
        }
        if (addHoursToISODate(rounded, this.#viewRange) < this._minDate) {
            rounded = addHoursToISODate(this._minDate, this.#viewRange)
        }

        this._dateInput.value = addMinutesToISODate(rounded, -getTZOffset(rounded))
        
        this.#updateLines(rounded, force).then()
    }

    updatePlotAndDimensions() {
        let width = window.getComputedStyle(this.#plotDiv).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        width = parseInt(width)
        const part = (width - 400) / (this.#plotBaseWidth - 400)
        this.#viewRange = this.#minViewRange + Math.round((this.#baseViewRange - this.#minViewRange) * part)
        this.updatePlot()

        // legend slides into plot even after updatePlot, seems like an oversight in Plotly.react, this solves it
        if (this.#plotDiv.layout !== undefined) Plotly.relayout(this.#plotDiv, this.#plotDiv.layout)
    }

    // construct elements
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max
        addMinutesToInputFloored10(this._dateInput, -60 * 24)

        fetchData(this._apiUrl + 'logo').then((resp) => {
            this.#logoImg.src = resp
        })

        this.updatePlotAndDimensions() // this also calls updatePlot

        this._dateInput.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, () => {
            addMinutesToInputFloored10(this._dateInput, 10)
            this.updatePlot()
        }, 100, "mavirForward")

        addIntervalToButton(this._backwardButton, () => {
            addMinutesToInputFloored10(this._dateInput, -10)
            this.updatePlot()
        }, 100, "mavirBackward")

        this.#legendCheckbox.checked = true
        this.#legendCheckbox.addEventListener("change", () => {
            this.#showLegend = this.#legendCheckbox.checked
            this.updatePlot()
        })

        window.addEventListener('resize', () => {
            clearTimeout(this.#resizeTimeout)
            this.#resizeTimeout = setTimeout(() => {
                this.updatePlotAndDimensions()
            }, 50)
        })
    }

    display() {
        this.updatePlotAndDimensions()
    }
}
