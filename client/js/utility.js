const apiUrl = 'https://8000-01hq0fcfq8q8tabmwrb7nb3x24.cloudspaces.litng.ai/'
let intervals = {}

/**
* Make a button "holdable", uses setInterval() with mouseup and mousedown events
* @param {HTMLElement} button - button to add interval execution to
* @param {Function} func - function to execute on button, anything like You would add to setInterval()
* @param {number} ms - how many ms between executions
* @param {string} intervalName - a name for Your interval, should be unique if You don't want collisions
*/
function addIntervalToButton(button, func, ms, intervalName) {
    intervals[intervalName] = null
    button.addEventListener("mousedown", () => {
        func()
        intervals[intervalName] = setInterval(() => {
            if (button.disabled) return
            func()
        }, ms)
    })
    button.addEventListener("mouseup", () => clearInterval(intervals[intervalName]))
    button.addEventListener("mouseleave", () => clearInterval(intervals[intervalName]))
}

/**
* Calculate min and max date based on status data returned by HUNELWAPI API
* @param {object} status - contains a status requests answer
* @returns {object} minDate and maxDate with those keys in ISO format
*/
function calcMinMaxDate(status) {
    data = status.data

    let minDate = '9999999999999999999999999999' // placeholder for string comparison
    let maxDate = '0000000000000000000000000000' // placeholder for string comparison

    for (let key in data) {
        let item = data[key]
        if (item.StartDate === 'NaT' || item.EndDate === 'NaT') continue
        minDate = minDate > item.StartDate ? item.StartDate : minDate
        maxDate = maxDate < item.EndDate ? item.EndDate : maxDate
    }

    minDate = minDate.replace(' ', 'T')
    maxDate = maxDate.replace(' ', 'T')

    return {
        'minDate': minDate,
        'maxDate': maxDate
    }
}

/**
* Validate that a date is in an interval based on a minimum and maximum date
* @param {string} date - date in ISO format
* @param {string} minDate - minimum date in ISO format
* @param {string} maxDate - maximum date in ISO format
* @returns {boolean} is date in the interval?
*/
function validDate(date, minDate, maxDate) {
    // validate date based on minDate and maxDate
    return minDate <= date && date <= maxDate
}

/**
* Get a values percentage position in a range specified by its minimum and maximum
* @param {number} min - start of interval
* @param {number} max - end of interval
* @param {number} value - number to get percentage position of
* @returns {number} percentage as float (e.g. 0.5 -> 50%)
*/
function getPercentageInRange(min, max, value) {
    // get percentage position of value in range specified by min and max
    let length = max - min
    let position = value - min
    return position / length
}

/**
* Turn array into rgba string format
* @param {Array} arr - 3 or 4 element array with RGB or RGBA elements
* @param {number} alpha - use alpha?, doesn't use alpha if it's null or undefined
* @returns {string} rgba color string
*/
function arrToRGBA(arr, alpha = null) {
    if (alpha === null || alpha === undefined) {
        alpha = arr[3]
    }
    return "rgba(" + arr[0] + "," + arr[1] + "," + arr[2] + "," + alpha + ")"
}

/**
* Get linear gradient at specified percentage, stops split evenly
* @param {Array} stops - nested array, where each element is an array of 4 specifying RGBA
* @param {number} value - float to get color at, between 0-1
* @returns {Array} 4 element array specifying RGBA color
*/
function linearGradient(stops, value) {
    // calculate linearGradient with value in [0,1] and given colorstops (even gradient)
    const stopLength = 1 / (stops.length - 1)
    const valueRatio = value / stopLength
    const stopIndex = Math.floor(valueRatio)
    if (stopIndex >= (stops.length - 1)) {
        return stops[stops.length - 1]
    } else if (stopIndex < 0) {
        return stops[0]
    }
    const stopFraction = valueRatio % 1
    return lerp(stops[stopIndex], stops[stopIndex + 1], stopFraction)
}

/**
* Interpolates between 2 arrays of 4 elements
* @param {Array} pointA - first point, 4 element array
* @param {Array} pointB - second point, 4 element array
* @param {number} normalValue - float between 0-1, interpolate A->B amount
* @returns {Array} interpolated 4 element array
*/
function lerp(pointA, pointB, normalValue) {
    return [
        pointA[0] + (pointB[0] - pointA[0]) * normalValue,
        pointA[1] + (pointB[1] - pointA[1]) * normalValue,
        pointA[2] + (pointB[2] - pointA[2]) * normalValue,
        pointA[3] + (pointB[3] - pointA[3]) * normalValue,
    ]
}

/**
* Async fetch of a URL from an API responding with a JSON
* @param {string} url - url to fetch
* @returns {Object} - response JSON
*/
async function fetchData(url) {
    // async fetch data from given url and handle errors
    // May throw errors while fetching!
    const response = await fetch(url)

    if (!response.ok) {
        throw new Error("Couldn't fetch " + url)
    }

    return await response.json()
}

/**
* Floor an ISO time to 10 minutes
* @param {string} timeString - ISO time string to floor
* @returns {string} ISO time string that's floored to 10 minutes
*/
function floorTo10Min(timeString) {
    // floor given timestring to 10 min
    const datetime = new Date(timeString);

    const roundedMinutes = Math.floor(datetime.getMinutes() / 10) * 10;

    return new Date(
        datetime.getFullYear(),
        datetime.getMonth(),
        datetime.getDate(),
        datetime.getHours(),
        roundedMinutes,
        0
    );
}

/**
* Floor an ISO time to hour
* @param {string} timeString - ISO time string to floor
* @returns {string} ISO time string that's floored to hour
*/
function floorToHour(timeString) {
    // floor given timestring to hour
    const datetime = new Date(timeString);

    return new Date(
        datetime.getFullYear(),
        datetime.getMonth(),
        datetime.getDate(),
        datetime.getHours(),
        0,
        0
    );
}

/**
* Convert Date to ISO string in UTC time
* @param {Date} localDate - date to convert
* @returns {string} ISO string in UTC time
*/
function localToUtcString(localDate) {
    // convert given Date to utcstring HTML elements understand
    return localDate.toISOString().replace(/.\d{3}Z$/, '') // Remove milliseconds and append 'Z'
}

/**
* Add hours to a date in ISO string format
* @param {string} date - date to add to
* @param {number} hours - hours to add
* @returns {string} modified ISO string
*/
function addHoursToISODate(date, hours) {
    let datetime = new Date(date)
    datetime.setHours(datetime.getHours() + hours - datetime.getTimezoneOffset() / 60)
    return localToUtcString(datetime)
}

/**
* Add minutes to a date in ISO string format
* @param {string} date - date to add to
* @param {number} minutes - minutes to add
* @returns {string} modified ISO string
*/
function addMinutesToISODate(date, minutes) {
    let datetime = new Date(date)
    datetime.setMinutes(datetime.getMinutes() + minutes - datetime.getTimezoneOffset())
    return localToUtcString(datetime)
}

/**
* Add minutes to an input's value after flooring it to 10 minutes
* @param {HTMLElement} dateInput - datetime-local element to add to
* @param {number} minutes - minutes to add after flooring
*/
function addMinutesToInputFloored10(dateInput, minutes) {
    // floors input value to 10 Min before addition 
    let rounded = floorTo10Min(dateInput.value + ":00")
    rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    rounded.setMinutes(rounded.getMinutes() + minutes)
    dateInput.value = localToUtcString(rounded)

}
