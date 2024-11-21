
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Container {
    @SerializedName("name")
    @Expose
    private String name;

    @SerializedName("port")
    @Expose
    private Integer port;

    @SerializedName("dataTransferPort")
    @Expose
    private Integer dataTransferPort;
}