from flask import Flask, request, render_template_string, jsonify
import requests

app = Flask(__name__)
MASTER_SERVER = "http://172.31.21.118:5000"

# In-memory store to track previous URL count
prev_url_counts = {}

HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <title>Distributed Monitor</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
  <style>
    #result-box {
      max-height: 250px;
      overflow-y: auto;
      border: 1px solid #ccc;
      padding: 10px;
      margin-top: 15px;
      background: #f9f9f9;
    }
    .thread-circle {
      display: inline-block;
      width: 16px;
      height: 16px;
      border-radius: 50%;
      margin-right: 4px;
    }
    .circle-running { background-color: #28a745; }
    .circle-idle { background-color: #6c757d; }
    .circle-unknown { background-color: #dc3545; }
  </style>
</head>
<body class="bg-light">
  <div class="container mt-5">
    <div class="card shadow-sm p-4">
      <h1 class="text-center mb-4">Distributed System Monitor</h1>

      <div class="row">
        <div class="col-md-6">
          <h5>🌐 Submit URL</h5>
          <input id="crawl-url" class="form-control mb-2" placeholder="Enter URL..." />
          <input id="crawl-depth" type="number" min="0" max="5" value="2" class="form-control mb-2" placeholder="Max Depth" />
          <!-- 🆕 Domain Restriction Radio -->
          <div class="form-check">
            <input class="form-check-input" type="radio" name="domainLimit" id="domainYes" value="true" checked>
            <label class="form-check-label" for="domainYes">Restrict to original domain</label>
          </div>
          <div class="form-check mb-2">
            <input class="form-check-input" type="radio" name="domainLimit" id="domainNo" value="false">
            <label class="form-check-label" for="domainNo">No restriction</label>
          </div>
          <button id="crawl-submit" class="btn btn-primary w-100">Submit</button>
          <div id="crawl-message" class="mt-2 text-info"></div>
        </div>
        <div class="col-md-6">
          <h5>🔍 Search</h5>
          <input id="search-keyword" class="form-control mb-2" placeholder="Enter keyword..." />
          <button id="search-submit" class="btn btn-success w-100">Search</button>
          <button id="clear-results" class="btn btn-outline-secondary mt-2 w-100">Clear Results</button>
          <div id="result-box" class="mt-2" style="display: none;"></div>
        </div>
      </div>

      <hr class="my-4" />
      <div class="d-flex justify-content-between align-items-center mb-2">
        <h4>Status Panel</h4>
        <div>
          <label for="detailedToggle">Detailed Monitoring:</label>
          <input type="checkbox" id="detailedToggle" />
        </div>
      </div>

      <h5>Crawlers</h5>
      <div id="crawler-panel"><p>Loading...</p></div>

      <h5 class="mt-4">Indexers</h5>
      <div id="indexer-panel"><p>Loading...</p></div>
    </div>
  </div>

  <script>
    let prevCounts = {};

    function renderThreads(threads_info, nodeStatus) {
      if (!Array.isArray(threads_info)) return "";
      return threads_info.map(t => {
        const status = (t.status || "").toLowerCase();
        let cls = "circle-unknown";

        if (nodeStatus === "not active") {
          cls = "circle-unknown"; // whole node is down
        } else if (status.includes("crawling") || status.includes("indexing")) {
          cls = "circle-running";
        } else if (status.includes("idle") || status.includes("waiting")) {
          cls = "circle-idle";
        }

        return `<span class="thread-circle ${cls}" title="${t.status}"></span>`;
      }).join('');
     }

    function renderPanel(data, targetId, detailed) {
      let html = `
        <table class="table table-bordered">
          <thead class="table-light">
            <tr>
              <th>Node ID</th>
              <th>IP</th>
              <th>Status</th>
              <th>URLs</th>
              <th>Last Seen</th>
              ${detailed ? '<th>Threads</th>' : ''}
            </tr>
          </thead>
          <tbody>`;

      if (data.length > 0) {
        data.forEach(row => {
          const nodeId = row.node_id;
          const current = row.url_count || 0;
          const previous = prevCounts[nodeId] || 0;

          // Detect running on client side
          let statusLabel = row.status;
          if (statusLabel !== "not active" && current > previous) {
            statusLabel = "running";
          }

          prevCounts[nodeId] = current;

          const badge = statusLabel === "running"
            ? '<span class="badge bg-success">Running</span>'
            : statusLabel === "idle"
              ? '<span class="badge bg-secondary">Idle</span>'
              : '<span class="badge bg-danger">Not Active</span>';

          html += '<tr>' +
                    `<td>${nodeId}</td>` +
                    `<td>${row.ip}</td>` +
                    `<td>${badge}</td>` +
                    `<td>${current}</td>` +
                    `<td>${row.last_seen}</td>` +
                    (detailed ? `<td>${renderThreads(row.threads_info, row.status.toLowerCase())}</td>` : '') +
                  '</tr>';
        });
      } else {
        html += '<tr><td colspan="6">No data available.</td></tr>';
      }

      html += '</tbody></table>';
      document.getElementById(targetId).innerHTML = html;
    }

    function fetchHeartbeat() {
      const detailed = $('#detailedToggle').is(':checked');
      const url = `/heartbeat?detailed=${detailed}`;
      $.get(url, function(data) {
        const crawlers = data.filter(row => row.role === "crawler");
        const indexers = data.filter(row => row.role === "indexer");
        renderPanel(crawlers, "crawler-panel", detailed);
        renderPanel(indexers, "indexer-panel", detailed);
      }).fail(() => {
        $("#crawler-panel").html("<div class='alert alert-warning'>Failed to fetch crawler status.</div>");
        $("#indexer-panel").html("<div class='alert alert-warning'>Failed to fetch indexer status.</div>");
      });
    }

    $(document).ready(function () {
      fetchHeartbeat();
      setInterval(fetchHeartbeat, 2000);

      $("#crawl-submit").click(function () {
        const url = $("#crawl-url").val().trim();
        const depth = $("#crawl-depth").val();
        const domainRestricted = $("input[name='domainLimit']:checked").val() === "true";

        if (!url) return $("#crawl-message").text("Please enter a URL.");

        $.ajax({
          url: "/crawl",
          method: "POST",
          contentType: "application/json",
          data: JSON.stringify({ url, max_depth: depth, domain_restricted: domainRestricted }),
          success: (res) => $("#crawl-message").text(res.message),
          error: (xhr) => {
            const msg = xhr.responseJSON?.error || "Failed to submit URL.";
            $("#crawl-message").text(msg);
          },
        });
      });



      $("#search-submit").click(function () {
        const keyword = $("#search-keyword").val().trim();
        if (!keyword) return;
        $.get("/search", { keyword }, function (data) {
          const urls = data.urls || [];
          if (urls.length === 0) {
            $("#result-box").html("❌ No results found.").show();
            return;
          }
          let html = "<ul class='list-group'>";
          urls.forEach(url => {
            html += `<li class='list-group-item'><a href='${url}' target='_blank'>${url}</a></li>`;
          });
          html += "</ul>";
          $("#result-box").html(html).show();
        });
      });

      $("#clear-results").click(function () {
        $("#result-box").html("").hide();
      });
    });
  </script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/heartbeat')
def heartbeat():
    try:
        detailed = request.args.get('detailed', 'false')
        response = requests.get(f"{MASTER_SERVER}/api/status?detailed={detailed}", timeout=5)
        return jsonify(response.json() if response.status_code == 200 else [])
    except:
        return jsonify([])

@app.route('/search')
def search():
    keyword = request.args.get("keyword", "")
    try:
        response = requests.get(f"{MASTER_SERVER}/api/search", params={"keyword": keyword}, timeout=5)
        return jsonify(response.json())
    except:
        return jsonify({'error': 'Failed to contact master.'}), 500

@app.route('/crawl', methods=['POST'])
def crawl():
    try:
        data = request.get_json()
        response = requests.post(f"{MASTER_SERVER}/api/crawl", json=data, timeout=5)
        return jsonify(response.json()), response.status_code
    except:
        return jsonify({'error': 'Failed to contact master.'}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5050, debug=False)
