package org.sunbird.org.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CustomeSelfRegistrationId implements Serializable {

    private String orgId;
    private String id;

    // Override equals() and hashCode()
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomeSelfRegistrationId that = (CustomeSelfRegistrationId) o;

        if (!orgId.equals(that.orgId)) return false;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = orgId.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }
}
