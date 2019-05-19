
class CircularBuffer {
    constructor(capacity) {
        this.max_size = capacity;
        this.size = 0;
        this.start_index = 0;
        this.data = new Array(this.max_size);
    }

    push_back(el) {
        let next_index = this.size % this.max_size;
        if (this.size === this.max_size) {
            this.start_index++;
        }
        else {
            this.size++;
        }
        this.data[next_index] = el;
    }

    get(index) {
        let actual_index = (index + this.start_index)%this.max_size;
        return this.data[actual_index]
    }
}


function escape_html(unsafe) {
    // From https://stackoverflow.com/questions/6234773/can-i-escape-html-special-chars-in-javascript
    return unsafe
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}


function get_html_for_update(this_update) {
    let url = this_update["new_val"]["url"];
    var time_str = null;
    var css_class = null;
    var action_str = null;



    if (this_update["type"] === "add") {
        time_str = this_update["new_val"]["created_at"];
        css_class = "";
        action_str = "Started request for"
    }
    else if (this_update["type"] === "change") {
        time_str = this_update["new_val"]["completed_at"];

        let did_fail = this_update["new_val"]["parsed_data"] === null;
        css_class = did_fail ? "list-group-item-danger" : "list-group-item-success";
        action_str = did_fail ? "Failed request for" : "Completed request for"
    }



    // Most of the escaping here is unnecessary, but it's good style
    let this_update_html = `
    <li class="list-group-item ${escape_html(css_class)}">
        <div class="request-item">
            <div class="request-item-url">${escape_html(action_str)} <a href="${escape_html(url)}">${escape_html(url)}</a></div>
            <div class="request-item-time">${escape_html(time_str)}</div>
        </div>
    </li>
    `;

    return this_update_html;

}


function redraw_events() {


    let all_update_htmls = [];
    for (let i = window.live_updates.size-1; i >= 0; i--) {
        let this_update = live_updates.get(i);
        all_update_htmls.push(get_html_for_update(this_update));
    }

    let full_html = all_update_htmls.join("\n");



    document.getElementById("live-updates-container").innerHTML = full_html;
}

function update_globals(event) {

    // Really this is wrong, need to query it once at start or somth
    console.log(event);
    if (event["type"] === "change") {
        window.num_pages_attempted++;
        window.num_bytes_saved += JSON.stringify(event["new_val"]["parsed_data"]).length;
    }

}


function redraw_stats() {
    document.getElementById("stats-pages-scraped").innerText = window.num_pages_attempted.toString();
    document.getElementById("stats-data-saved").innerText = (window.num_bytes_saved/1e3).toFixed(2) + " KB";
}

function ws_message_recieved(event) {

    console.log("REcieved", event);
    let parsed_evt = JSON.parse(event.data);

    update_globals(parsed_evt);
    window.live_updates.push_back(parsed_evt);
    redraw_events();
    redraw_stats();

}

function init_websocket_connection() {
    let ws_full_url = `ws://${document.location.host}/websocket`;
    let ws = new WebSocket(ws_full_url);

    ws.onmessage = ws_message_recieved;
    let NUM_EVENTS_SAVED = 50;
    window.live_updates = new CircularBuffer(NUM_EVENTS_SAVED);
    window.num_pages_attempted = 0;
    window.num_bytes_saved = 0;
}

init_websocket_connection();

