package fr.gouv.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HBaseRow {



        private String key;
        private Double nombre_aides;
        private Double montant_total;
        private String libelle_region;
    }

