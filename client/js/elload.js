class MavirController {
    // constants
    #forwardButton = document.getElementById("mavirForwardButton")
    #backwardButton = document.getElementById("mavirBackwardButton")
    #dateInput = document.getElementById("mavirDateInput")
    #legendCheckbox = document.getElementById("mavirShowLegend")
    #urlA = document.getElementById("mavirUrlA")
    #logoImg = document.getElementById("mavirLogo")
    #plotDivId = "mavirPlotDiv"
    #loadingOverlay = document.getElementById("mavirLoadingOverlay")
    #plotBaseWidth = 1080 // maximal width defined via css
    #baseViewRange = 6
    #minViewRange = 2

    // variables
    #minDate = null
    #maxDate = null
    #requestedMinDate = null
    #requestedMaxDate = null
    #status = null
    #data = null
    #viewRange = this.#baseViewRange
    #resizeTimeout = null
    #showLegend = true

    // plot format
    plotFormat = {
        // net load
        NetSystemLoad: {
            name: langStringText('NetSystemLoad'),
            color: 'rgb(102, 68, 196)',
            dash: 'solid'
        },
        NetSystemLoadFactPlantManagment: {
            name: langStringText('NetSystemLoadFactPlantManagment'),
            color: 'rgb(136, 204, 238)',
            dash: 'solid'
        },
        NetSystemLoadNetTradeSettlement: {
            name: langStringText('NetSystemLoadNetTradeSettlement'),
            color: 'rgb(68, 170, 153)',
            dash: 'solid'
        },
        NetPlanSystemLoad: {
            name: langStringText('NetPlanSystemLoad'),
            color: 'rgb(17, 119, 51)',
            dash: 'solid'
        },
        NetSystemLoadDayAheadEstimate: {
            name: langStringText('NetSystemLoadDayAheadEstimate'),
            color: 'rgb(153, 153, 51)',
            dash: 'solid'
        },
        // production
        NetPlanSystemProduction: {
            name: langStringText('NetPlanSystemProduction'),
            color: 'rgb(255, 255, 255)',
            dash: 'dashdot'
        },
        // gross load
        GrossSystemLoad: {
            name: langStringText('GrossSystemLoad'),
            color: 'rgb(204, 102, 119)',
            dash: 'dash'
        },
        GrossCertifiedSystemLoad: {
            name: langStringText('GrossCertifiedSystemLoad'),
            color: 'rgb(136, 34, 85)',
            dash: 'dash'
        },
        GrossPlanSystemLoad: {
            name: langStringText('GrossPlanSystemLoad'),
            color: 'rgb(170, 68, 153)',
            dash: 'dash'
        },
        GrossSystemLoadDayAheadEstimate: {
            name: langStringText('GrossSystemLoadDayAheadEstimate'),
            color: 'rgb(221, 204, 119)',
            dash: 'dash'
        },
    }

    // functions
    #mavirNavDisabled(disabled) {
        this.#forwardButton.disabled = disabled
        this.#backwardButton.disabled = disabled
        this.#loadingOverlay.className = disabled ? "loading" : ""
    }

    async updateStatus() {
        this.#status = await fetchData(apiUrl + 'mavir/status')
    }

    #makeLines(from, to) {
        // update mavir lineplot with given range, expects: from < to
        this.#urlA.href = this.#data.Message.match(/\(([^)]+)\)/)[1]

        let data = this.#data.data
        let x = []
        let ys = {}

        for (let key in this.plotFormat) {
            ys[key] = []
        }

        for (let i = from; i <= to; i = addMinutesToISODate(i, 10)) {
            let item = data[i]

            // display date in local time
            let date = new Date(i)
            date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
            x.push(localToUtcString(date).replace('T', ' '))
            for (let fet in this.plotFormat) {
                ys[fet].push(item[fet])
            }

        }

        let plotData = []

        for (let fet in this.plotFormat) {
            let format = this.plotFormat[fet]
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
                }
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
                'autoScale2d',
                'resetScale2d'
            ]
        }

        Plotly.newPlot(this.#plotDivId, plotData, plotLayout, plotConfig)
    }

    async #updateLines(datetime) {
        // update elload centered on given datetime
        if (this.#status === null) {
            await this.updateStatus()
        }

        let from = addHoursToISODate(datetime, -this.#viewRange)
        let to = addHoursToISODate(datetime, this.#viewRange)

        let reRequest = false
        if (this.#requestedMinDate === null || this.#requestedMaxDate === null) {
            // setting a smaller range to reduce traffic
            this.#requestedMinDate = addHoursToISODate(datetime, -10)
            this.#requestedMaxDate = addHoursToISODate(datetime, 10)

            reRequest = true
        } else if ((from < this.#requestedMinDate) || (to > this.#requestedMaxDate)) {
            this.#requestedMinDate = addHoursToISODate(datetime, -24)
            this.#requestedMaxDate = addHoursToISODate(datetime, 24)

            if (this.#requestedMaxDate > this.#maxDate) {
                this.#requestedMaxDate = this.#maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this.#mavirNavDisabled(true)

            this.#data = await fetchData(
                apiUrl + "mavir/load?start_date=" + this.#requestedMinDate + "&end_date=" + this.#maxDate
            )

            this.#mavirNavDisabled(false)
        }

        this.#makeLines(from, to)
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

        this.#updateLines(datetime).then()
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

    updateMavirPlotDimensions() {
        const width = window.getComputedStyle(document.getElementById(this.#plotDivId)).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        const part = (width - 400) / (this.#plotBaseWidth - 400)
        this.#viewRange =this.#minViewRange + Math.round((this.#baseViewRange - this.#minViewRange) * part)
        this.updatePlot()
    }

    // construct elements
    async setup() {
        // setup function, call before use
        await this.updateStatus()
        this.update()
        this.#dateInput.value = this.#dateInput.max
        addMinutesToInputRounded10(this.#dateInput, -60 * 24)

        fetchData(apiUrl + 'mavir/logo').then((resp) => {
            this.#logoImg.src = resp
        })

        this.updatePlot()

        this.#dateInput.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this.#forwardButton, () => {
            addMinutesToInputRounded10(this.#dateInput, 10)
            this.updatePlot()
        }, 100, "mavirForward")

        addIntervalToButton(this.#backwardButton, () => {
            addMinutesToInputRounded10(this.#dateInput, -10)
            this.updatePlot()
        }, 100, "mavirForward")

        this.#legendCheckbox.checked = true
        this.#legendCheckbox.addEventListener("change", () => {
            this.#showLegend = this.#legendCheckbox.checked
            this.updatePlot()
        })

        window.addEventListener('resize', () => {
            clearTimeout(this.#resizeTimeout)
            this.#resizeTimeout = setTimeout(() => {
                this.updateMavirPlotDimensions()
            }, 50)
        })
    }

    switch() {
        this.update()
        this.updateMavirPlotDimensions()
        this.updatePlot()
    }
}
