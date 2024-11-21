
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ParamList {
    @SerializedName("name")
    @Expose
    private String name;

    @SerializedName("value")
    @Expose
    private String value;
}
