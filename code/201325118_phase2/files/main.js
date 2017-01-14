document.onkeypress = function (e) {
    e = e || window.event;
    $('#user_input').focus();
    $('#user_input').click();
    // getStringFromUser();
};
	$('#user_input').click(function() {
		$('#top_bar').slideUp(400);
		$('#logo_small').show();
		$('#logo_small').animate({opacity: 1}, 500);
		$('#user_input').animate({width: "75%"}, 500, function() {
		$('#top_bar').css({"position":"fixed"});
		});
	});
	$('#user_input').keyup(function(e) {
    if (e.which == 13) {
    	getStringFromUser();
    }
});
	function search(q) {
		var result = "";
		$('#user_input').val(q);
		$(function() {
        $.getJSON('/search', {
          query: q
        }, function(data) {
        	console.log(data);
        	if (data["suggest"]!=$('#user_input').val().toLowerCase()) {
        		result += "<span class='stats'>Did you mean <i><u id='suggestion' onclick='search(\""+data["suggest"]+"\")'>"+data["suggest"]+"</u></i>?</span>";
        	}
        	result += "<span class='stats'>Generated "+data["number"]+" results in "+data["time"]+" seconds</span><br />";
        	$.each(data["data"], function(key, val) {
	        	result += "<ul class='resultbox'>";
	        	count = 0;
	        	$.each(val, function(i, j) {
	        		count += 1
	        		if (count < 20) {
		        		result += "<li>"+j+"</li>";
	        		}
	        		else {
	        			return;
	        		}
	        	});	
				switch(key) {
	        		case 'i':
	        		result += "<li class='small'>Infobox</li>";
	        		break;
	        		case 'b':
	        		result += "<li class='small'>Page text</li>";
	        		break;
	        		case 't':
	        		result += "<li class='small'>Page title</li>";
	        		break;
	        		case 'c':
	        		result += "<li class='small'>Categories</li>";
	        		break;
	        		case 'r':
	        		result += "<li class='small'>References</li>";
	        		break;
	        		case 'l':
	        		result += "<li class='small'>Links</li>";
	        		break;
	        	}
	        	result += "</ul>";
        	});
        	// result += JSON.stringify(data, null, '\t\t\t');
          $("#results").html(result);
        });
        return false;
    });
	}
	function getStringFromUser() {
		console.log("Searching...");
		search($('#user_input').val());
	}