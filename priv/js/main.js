$(document).ready(function() {
  $("#header").hide();
  $("#controls").hide();
  $("#graph").hide();

  $.ajax({
      type: "GET",
      url: "/api/plots",
      dataType: "json",
      success: function (data) {
        $.each(data.plots, function(i, data) {
          var elem = "<li class='plot'><object type='application/pdf' data='/plots/" + data + "' width='100%' height='100%'><p>hi</p></object></li>";
          $(elem).appendTo('#plots');
        });
    }
  });

  $.ajax({
      type: "GET",
      url: "/api/logs",
      dataType: "json",
      success: function (data) {
        $.each(data.logs, function(i, data) {
          var elem = "<li><a href='/logs/" + data + "'>"+data+"</a></li>";
          $(elem).appendTo('#logs');
        });
    }
  });


  $.get("/api/dag", function(data) {
      var contents = data.dot_content || [];
      var stringContents = contents.map(function(e) {
          return String.fromCharCode(e);
      }).join("");

      $(".dag").empty();
      $(".dag").append(Viz(stringContents));

      setInterval(function() {
          $.get("/api/dag", function(data) {
              var contents = data.dot_content || [];
              var stringContents = contents.map(function(e) {
                  return String.fromCharCode(e);
              }).join("");

              $(".dag").empty()
              $(".dag").append(Viz(stringContents));
            });
        }, 10000);
  });

  $("#logo").fadeOut("slow", function() {

    $("#header").fadeIn();
    $("#controls").fadeIn();
    $("#graph").fadeIn();

    var width = 300,
        height = 250;

    var color = d3.scale.category10();

    var nodes = [],
        links = [];

    var force = d3.layout.force()
        .nodes(nodes)
        .links(links)
        .charge(-400)
        .linkDistance(120)
        .size([width, height])
        .on("tick", tick);

    var svg = d3.select("#topology").append("svg")
        .attr("width", width)
        .attr("height", height);

    var node = svg.selectAll(".node"),
        link = svg.selectAll(".link");

    var update = function(response) {

        // Add any missing nodes.
        $.each(response.nodes, function(key, value) {
            var result = findNode(nodes, value.id);
            if (result == undefined) {
                nodes.push(value);
            }
        });

        // Remove deleted nodes.
        $.each(nodes, function(key, value) {
            // Prevent from splice during iteration by ignoring
            // undefined values.
            if(value != undefined) {
                var result = findNodeIndex(response.nodes, value.id);
                if (result == undefined) {
                    nodes.splice(key, 1);
                }
            }
        });

        // Add any missing links.
        $.each(response.links, function(key, value) {
            var result = findLink(links, value.source, value.target);
            var sourceNode = findNode(nodes, value.source);
            var targetNode = findNode(nodes, value.target);
            if (result == undefined) {
                links.push({source: sourceNode, target: targetNode});
            }
        });

        // Remove deleted links.
        for(var i = links.length; i--;){
            // Prevent from splice during iteration by ignoring
            // undefined values.
            if(links[i] != undefined) {
                if(typeof links[i].source === 'object') {
                    var result = findLinkIndex(response.links, links[i].source.id, links[i].target.id);
                } else {
                    var result = findLinkIndex(response.links, links[i].source, links[i].target);
                }
                if (result == undefined) {
                    links.splice(i, 1);
                }
            }
        }

        start();
    };

    // Invoke immediately and schedule refreshes.
    $.get("/api/status", function(response) {
        update(response);

        setInterval(function() {
            $.get("/api/status", function(response) {
                update(response);
            });
        }, 2000);
    });

    function start() {
      link = link.data(force.links(), function(d) { return d.source.id + "-" + d.target.id; });
      link.enter().insert("line", ".node").attr("class", "link");
      link.exit().remove();

      node = node.data(force.nodes(), function(d) { return d.id;});
      node.enter().append("circle").attr("class", function(d) { return "node " + d.id; }).attr("r", 8).style("fill", function(d) { return color(d.group); })
      node.exit().remove();

      force.start();
    }

    function tick() {
      node.attr("cx", function(d) { return d.x; })
          .attr("cy", function(d) { return d.y; })

      link.attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });
    }

    // Taken from: http://stackoverflow.com/questions/9539294/adding-new-nodes-to-force-directed-layout
    var findNode = function (nodes, id) {
        for (var i=0; i < nodes.length; i++) {
            if (nodes[i].id === id)
                return nodes[i]
        };
    }

    var findLink = function (links, source, target) {
        for (var i=0; i < links.length; i++) {
            if (links[i].source.id === source && links[i].target.id === target)
                return links[i]
        };
    }

    // Taken from: http://stackoverflow.com/questions/9539294/adding-new-nodes-to-force-directed-layout
    var findNodeIndex = function (nodes, id) {
        for (var i=0; i < nodes.length; i++) {
            if (nodes[i].id === id)
                return i
        };
    }

    var findLinkIndex = function (links, source, target) {
        for (var i=0; i < links.length; i++) {
            if (links[i].source === source && links[i].target === target)
                return i
        };
    }

  });
});
