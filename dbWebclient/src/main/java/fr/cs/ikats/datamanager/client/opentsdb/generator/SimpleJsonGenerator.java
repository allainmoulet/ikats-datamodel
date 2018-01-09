/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 *
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 */

package fr.cs.ikats.datamanager.client.opentsdb.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import fr.cs.ikats.datamanager.DataManagerException;

/**
 * generate a Json import request from a list of points
 */
public class SimpleJsonGenerator {

    private static final Logger LOGGER = Logger.getLogger(AdvancedJsonGenerator.class);

    /**
     * Constantes fixant les positions des champs de la liste de points
     */
    static final String KEY_METRIQUE = "metric";
    static final String KEY_TIME = "timestamp";
    static final String KEY_VAL = "value";
    static final String KEY_TAGS = "tags";
    // nom du second tag. le nom du tag principal peut etre passe dans le
    // constructeur
    static final String KEY_SECOND_TAG = "numero";
    static final int DIGITS_SECONDE = 10;
    static final int DIGITS_MILLISECONDE = 13;

    // n° de la colonne utilisee pour generer le nom du metrique
    static final int POSITION_METRIQUE = 0;
    // n° de la colonne utilisee comme nom de tag principal; -1 si absent
    static final int POSITION_NOM_TAG = -1;
    // n° de la colonne utilisee comme valeur du tag principal; -1 si absent
    // (valeur de tag incluse dans le 1er champ)
    static final int POSITION_VAL_TAG = -1;
    // n° de la colonne utilisee comme timestamp de debut
    static final int POSITION_TIME = 1;
    // n° de la colonne utilisee comme 1ere valeur
    static final int POSITION_VAL = 2;

    List<String> mots_cles = null;
    /**
     * ligne contenant la liste des points
     */
    private String listePoints;
    private String[] splittedLine;

    private String libelle;
    /**
     * periode de la sequence en secondes
     */
    private int periode;
    /**
     * flag indiquant si la serie comporte plusieurs un seul timestamp, donc
     * forcement periodique, ou non
     */
    private Boolean periodique;
    /**
     * nom du tag principal
     */
    private String nomEtiquette;
    /**
     * utilisé pour générer le nom du metrique
     */
    private String jeuDonnees;

    /**
     * @param periodique the periodique to set
     */
    public void setPeriodique(Boolean periodique) {
        this.periodique = periodique;
    }

    /**
     * @param nomEtiquette the nomEtiquette to set
     */
    public void setNomEtiquette(String nomEtiquette) {
        this.nomEtiquette = nomEtiquette;
    }

    /**
     * @param jeuDonnees the jeuDonnees to set
     */
    public void setJeuDonnees(String jeuDonnees) {
        this.jeuDonnees = jeuDonnees;
    }

    /**
     *
     */
    protected SimpleJsonGenerator() {
        mots_cles = new ArrayList<>();
    }

    /**
     * @param period periode de la sequence en seconde; si &lt;=0, on suppose que les
     *               timestamps sont renseignés pour chaque point
     */
    public SimpleJsonGenerator(int period) {
        this();
        this.periode = period;
    }

    /**
     * @param dataSet the dataset
     * @param period  periode de la sequence en seconde; si &lt;=0, on suppose que les timestamps sont renseignés pour chaque point
     * @param tagName nom de l'etiquette principale; si etiquettes multiples, les
     *                noms des etiquettes doivent etre presents dans la sequence
     */
    public SimpleJsonGenerator(String dataSet, int period, String tagName) {
        this(period);
        this.nomEtiquette = tagName;
        this.jeuDonnees = dataSet;
    }

    /**
     * generate JSON
     *
     * @param input the input string
     * @return a string
     * @throws DataManagerException
     * @throws Exception            if error occurs
     */
    @SuppressWarnings("unchecked")
    public String generate(String input) throws DataManagerException {
        // decoupage ligne
        this.listePoints = input;
        lineToArray();
        checkCoherence();
        // creation du tableau Json
        JSONArray points = new JSONArray();
        JSONObject p = new JSONObject();
        initTags(p);

        // 1er point
        if (checkVal(splittedLine[POSITION_VAL].trim())) {
            p.put(KEY_TIME, splittedLine[POSITION_TIME]);
            p.put(KEY_VAL, splittedLine[POSITION_VAL]);
            points.add(p.clone());
        }
        // boucle sur les points (on suppose que POSITION_TIME < POSITION_VAL)
        int maxIndice = (this.periodique) ? splittedLine.length - POSITION_VAL : (splittedLine.length - POSITION_TIME) / 2;
        LOGGER.trace("maxIndice = " + maxIndice + " Periodique = " + periodique);
        for (int i = 1; i < maxIndice; i++) {
            if (this.periodique) {
                if (checkVal(splittedLine[POSITION_VAL + i].trim())) {
                    p.put(KEY_TIME, Integer.valueOf(splittedLine[POSITION_TIME]) + (i * this.periode));
                    p.put(KEY_VAL, splittedLine[POSITION_VAL + i]);
                    points.add(p.clone());
                }
            } else {
                if (checkVal(splittedLine[POSITION_VAL + i * 2].trim())) {
                    p.put(KEY_TIME, splittedLine[POSITION_TIME + i * 2]);
                    p.put(KEY_VAL, splittedLine[POSITION_VAL + i * 2]);
                    points.add(p.clone());
                }
            }
        }
        return points.toJSONString();
    }

    /**
     * prerequis : le nom du metrique dans le fichier est de la forme
     * nom1_(nom2)_mainTagValue_numero
     *
     * @param p objet point
     */
    @SuppressWarnings("unchecked")
    private void initTags(JSONObject p) {
        StringBuilder metricName = new StringBuilder();
        String numSerie = null;
        String mainTagValue = null;
        String[] metricParts = splittedLine[POSITION_METRIQUE].split("_");
        int nbParts = metricParts.length;
        // cas d'un nom de metrique specifique (contient un mot key; SPECIFIQUE
        // POC !!!)
        if (mots_cles.contains(metricParts[0])) { // le dernier element de
            // metricParts correspond au
            // tag principal
            numSerie = "00000";
            mainTagValue = metricParts[nbParts - 1];
            metricName.append(metricParts[0].toLowerCase()).append('.').append(metricParts[1]);

        } else { // le dernier element de metricParts correspond au tag secondaire
            // et l'avant dernier au tag principal
            numSerie = metricParts[nbParts - 1];
            mainTagValue = metricParts[nbParts - 2];
            if (jeuDonnees != null)
                metricName.append(this.jeuDonnees).append('.').append(metricParts[0]);
            else
                metricName.append(metricParts[0]);
        }

        p.put(KEY_METRIQUE, metricName.toString());
        // gestion de 2 etiquettes
        JSONObject tags = new JSONObject();
        tags.put(KEY_SECOND_TAG, numSerie);
        tags.put(this.nomEtiquette, mainTagValue);

        p.put(KEY_TAGS, tags);
        this.libelle = metricName + " / " + mainTagValue + " / " + numSerie;
    }

    private boolean checkVal(String valToCheck) {
        boolean checked = false;
        // check number. prise en compte notation scifi
        if (Pattern.matches("^(\\+|-)?[0-9]+(\\.[0-9]+e(\\+|-)[0-9]{2})?$", valToCheck))
            checked = true;

        return checked;
    }

    /**
     * verifie coherence des attributs : periode, résolution et etiquette
     * verifie si presence de plusieurs timestamps dans la ligne. si oui, flag
     * periodique à false ne foctionne que si POSITION_TIME et POSITION_VAL
     * correspondent à la réalité de l'entrée
     *
     * @throws DataManagerException
     */
    private void checkCoherence() throws DataManagerException {
        // detection si serie avec 1 timestamp par point ou non. Dans le cas ou
        // 1 timestamp/point(non periodique),
        // on suppose que les 4 1ers digits sont egaux
        if (splittedLine[POSITION_VAL + 1].length() == splittedLine[POSITION_TIME].length()
                && splittedLine[POSITION_VAL + 1].substring(0, 4).equals(splittedLine[POSITION_TIME].substring(0, 4)))
            this.periodique = Boolean.FALSE;
        else
            this.periodique = Boolean.TRUE;
        // set precision
        if (splittedLine[POSITION_TIME].length() > DIGITS_SECONDE) {
            setPrecision(DIGITS_MILLISECONDE);
            setPeriode(this.periode * 1000);
        } else {
            setPrecision(DIGITS_SECONDE);
        }
        if ((this.nomEtiquette == null && POSITION_NOM_TAG < 0) || (this.periodique && this.periode <= 0)) {
            throw new DataManagerException("generation impossible : tag principal ou periode non definie");
        }
    }

    /**
     * explose la ligne en tokens. Separateurs autorises : ; OU : OU space
     */
    private void lineToArray() {
        String sep;
        if (listePoints.contains(";"))
            sep = ";";
        else if (listePoints.contains(":"))
            sep = ":";
        else {
            sep = " ";
        }
        // remplacement des decimales avec des virgules
        if (listePoints.contains(","))
            this.splittedLine = listePoints.replaceAll("\\s+", " ").replace(',', '.').split(sep);
        else
            this.splittedLine = listePoints.replaceAll("\\s+", " ").split(sep);

        listePoints = null;
    }

    /**
     * @param precision the precision to set
     */
    public void setPrecision(int precision) {
    }

    /**
     * @return the periode
     */
    public int getPeriode() {
        return periode;
    }

    /**
     * @param periode the periode to set
     */
    public void setPeriode(int periode) {
        this.periode = periode;
    }

    /**
     * @return the libelle
     */
    public String getLibelle() {
        return libelle;
    }

    /**
     * @param mots_cles the mots_cles to set
     */
    public void setMots_cles(List<String> mots_cles) {
        this.mots_cles = mots_cles;
    }

}

