const apiUrl = "https://8000-01hq0fcfq8q8tabmwrb7nb3x24.cloudspaces.litng.ai/"

class UniqueID {
    static next() {
        if (UniqueID._id) {
            return ++UniqueID._id
        }
        UniqueID._id = 1
        return UniqueID._id
    }
}

let _intervals = {}
/**
* Make a button "holdable", uses setInterval() with mouseup and mousedown events
* @param {HTMLElement} button - button to add interval execution to
* @param {Function} func - function to execute on button, anything like You would add to setInterval()
* @param {number} ms - how many ms between executions
* @param {string} intervalName - a name for Your interval, should be unique if You don't want collisions
*/
function addIntervalToButton(button, func, ms, intervalName) {
    _intervals[intervalName] = null

    let removeInterval = () => clearInterval(_intervals[intervalName])

    let addInterval = () => {
        // just in case it gets stuck, another press will remove the interval
        if (_intervals[intervalName] !== null) {
            clearInterval(_intervals[intervalName])
        }

        func()
        _intervals[intervalName] = setInterval(() => {
            if (button.disabled) removeInterval()
            func()
        }, ms)
    }

    button.addEventListener("mousedown", addInterval)
    button.addEventListener("touchstart", addInterval)
    button.addEventListener("mouseup", removeInterval)
    button.addEventListener("touchend", removeInterval)
    button.addEventListener("mouseleave", removeInterval)
    button.addEventListener("touchcancel", removeInterval)
}

/**
* Calculate min and max date based on status data returned by HUNELWAPI API
* @param {object} status - contains a status requests answer
* @returns {object} minDate and maxDate with those keys in ISO format
*/
function calcMinMaxDate(status) {
    data = status.data

    let minDate = "9999999999999999999999999999" // placeholder for string comparison
    let maxDate = "0000000000000000000000000000" // placeholder for string comparison

    for (let key in data) {
        let item = data[key]
        if (item.StartDate === "NaT" || item.EndDate === "NaT") continue
        minDate = minDate > item.StartDate ? item.StartDate : minDate
        maxDate = maxDate < item.EndDate ? item.EndDate : maxDate
    }

    minDate = minDate.replace(" ", "T")
    maxDate = maxDate.replace(" ", "T")

    return {
        "minDate": minDate,
        "maxDate": maxDate
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
* Async fetch of a URL from an API responding with a JSON
* @param {string} url - url to fetch
* @async
* @returns {Promise<Object>} response JSON
*/
async function fetchData(url) {
    // async fetch data from given url and handle errors
    // May throw errors while fetching!
    let response = await fetch(url)

    if (response.status == 429) {
        // Largest rate limit in API is 2 seconds as of writing
        console.log("Status code 429 Too Many Requests, retrying in 3 seconds")
        await new Promise(r => setTimeout(r, 3000)) // "sleep"
        response = await fetch(url)
    }

    if (!response.ok) {
        throw new Error("Couldn't fetch " + url)
    }

    return await response.json()
}

/**
* Get current timezones offset
* @param {string} timeString - ISO time string to use (needed for daylight savings time offset)
* @returns {number} timezone offset in minutes
*/
function getTZOffset(timeString) {
    return (new Date(timeString)).getTimezoneOffset()
}

/**
* Floor an ISO time to 10 minutes
* @param {string} timeString - ISO time string to floor
* @param {number} minutes - minutes to floor to (1-60)
* @returns {string} ISO time string that's floored to 10 minutes
*/
function floorToMinutes(timeString, minutes) {
    // floor given timestring to 10 min
    const datetime = new Date(timeString);

    const roundedMinutes = Math.floor(datetime.getMinutes() / minutes) * minutes;

    return localToUtcString(new Date(
        datetime.getFullYear(),
        datetime.getMonth(),
        datetime.getDate(),
        datetime.getHours(),
        roundedMinutes,
        0
    ));
}

/**
* Convert Date to ISO string in UTC time
* @param {Date} localDate - date to convert
* @returns {string} ISO string in UTC time
*/
function localToUtcString(localDate) {
    // convert given Date to utcstring HTML elements understand
    return localDate.toISOString().replace(/.\d{3}Z$/, "") // Remove milliseconds and append 'Z'
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
* @param {number} floorTo - minutes to floor to
* @param {number} minutes - minutes to add after flooring
*/
function addMinutesToInputFloored(dateInput, floorTo, minutes) {
    // floors input value to 10 Min before addition
    let rounded = floorToMinutes(dateInput.value + ":00", floorTo)
    rounded = addMinutesToISODate(rounded, minutes)
    dateInput.value = addMinutesToISODate(rounded, -getTZOffset(rounded))
}
