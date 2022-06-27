import com.google.common.base.Splitter;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.json.JSONObject;
import org.apache.crunch.Pair;
import org.json.JSONException;

import java.util.*;
public class GetWords extends DoFn<String, Pair<String, String>> {
    private static final Splitter SPLITTER = Splitter
            .onPattern("\\n")
            .omitEmptyStrings();

    public void process(String line, Emitter<Pair<String, String>> emitter) {
        String concat_string;

        for (String word : SPLITTER.split(line)) {
            JSONObject obj = new JSONObject(word);
            if(obj.has("brand"))
                concat_string="0"+obj.getString("brand");
            else if(obj.has("brand_name"))
                concat_string="1"+obj.getString("brand_name");
            else if(obj.has("category"))
                concat_string="2"+obj.getString("category");
            else
                concat_string="3"+obj.getString("category_name");
            String product_id=obj.getString("product_id");
            emitter.emit(Pair.of(product_id, concat_string));
        }
    }
}