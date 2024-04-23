class AIController extends LinePlotController {

    /**
    * Sets up all elements of the controller, adds event listeners and display plot with max available dates visible
    * @async
    * @param {Object} index - JSON return of index page containing last update time under lastUpdateKey
    */
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max
        addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize * this._maxViewRange)

        await this.updatePlotAndDimensions() // this also calls updatePlot

        this._dateInput.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, async () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, this._stepSize)
            await this.updatePlot()
        }, 150, "aiForward")

        addIntervalToButton(this._backwardButton, async () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize)
            await this.updatePlot()
        }, 150, "aiBackward")

        window.addEventListener("resize", () => {
            clearTimeout(this._resizeTimeout)
            this._resizeTimeout = setTimeout(() => {
                this.updatePlotAndDimensions()
            }, 50)
        })
    }
}

