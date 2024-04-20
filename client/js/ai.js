class AIController extends LinePlotController {

    // construct elements
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max
        addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize * this._maxViewRange)

        this.updatePlotAndDimensions() // this also calls updatePlot

        this._dateInput.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, this._stepSize)
            this.updatePlot()
        }, 150, "aiForward")

        addIntervalToButton(this._backwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize)
            this.updatePlot()
        }, 150, "aiBackward")

        window.addEventListener("resize", () => {
            clearTimeout(this._resizeTimeout)
            this._resizeTimeout = setTimeout(() => {
                this.updatePlotAndDimensions()
            }, 50)
        })
    }
}

